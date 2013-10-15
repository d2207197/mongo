#!/usr/bin/env python
'''
Put ngram and count to HBase.

Input files format:
  a b c d[TAB]count
  x y z[TAB]count
  ...

For local:
 $ python hbase_put_ngramcnt.py hadoop.nlpweb.org bnc-all-cnt-ngram \\
        BNC_Bi_Cnt BNC_Tri_Cnt ...

For Hadoop:
  $ yarn jar $HADOOP_MAPRED_HOME/hadoop-streaming.jar \\
        -input /CORPUS/Web1T/Web1T/3gms \\
        -output 3gms \\
        -file hadoop_hbase_put_ngramcnt.py \\
        -mapper 'python hbase_put_ngramcnt.py hadoop.nlpweb.org bnc-all-cnt-ngram'
        -reducer cat \\


Create HBase table for this script:
  $ hbase shell
  hbase(main):001:0> create 'web1t-ngram-cnt', {NAME => '1', VERSIONS => '1'}, \\
        {NAME => '2', VERSIONS => '1'}, \\
        {NAME => '3', VERSIONS => '1'}, \\
        {NAME => '4', VERSIONS => '1'}, \\
        {NAME => '5', VERSIONS => '1'}
'''
# import hashlib
import json
import re
from functools import partial
# import happybase
import fileinput
# import struct
from copy import copy
import logging
reload(logging)
logging.basicConfig(level=logging.INFO, format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# from sys import exit

def parse_args():
    from argparse import ArgumentParser, RawTextHelpFormatter
    # import argparse
    # import sys

    parser = ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)

    parser.add_argument('HOST', nargs=1, help='mongodb server hostname')
    parser.add_argument('DATABASE', nargs=1, help='mongodb database name')
    parser.add_argument('COLLECTION', nargs=1, help='mongodb collection name')
    parser.add_argument('FILES', nargs='*', help='''input files.
    With no FILE, or when FILE is -, read standard input''', default='-')
    # parser.add_argument('-c', '--batch-count', nargs='?', const = 300, action="store", default = 300, type=int, help='Number of rows be sent to hbase per commit. Default is 300')

    parser.add_argument('-n', '--no-filter', action="store_true",  help='Toggle the filter')
    parser.add_argument('-a', '--auth', nargs=3,  help='''mongodb authenticate information''', metavar=('AUTH_DB', 'USER', 'PASSWORD'))

    return  parser.parse_args()


def selected_ngram(ngram, sel):
    """
    Arguments:
    - `ngram`:
    - `sel`:
    """
    sel_ngram = []
    for s in sel:
        sel_ngram.append(ngram[s])
    return sel_ngram

def unselected_ngram(ngram,sel):
    unsel = range(len(ngram))
    for s in sel:
        unsel.remove(s)
    return selected_ngram(ngram, unsel)


def to_rowkey(ngram, sel):
    selected_words = ' '.join(selected_ngram(ngram, sel))
    # if len(ngram) == len(sel):
        # return prefix + packed_cnt
    # suffix = hashlib.sha1(' '.join(unselected_ngram(ngram, sel))).digest()[:4]
    return selected_words

def to_column(ngm_len, sel):
    return str(ngm_len) + ':' + ''.join(map(str, sel))


# lemma_names = json.load(open('wordnet_bnc_lemma_names.json'))
web1t_unigrams_11000up = json.load(open('web1t_unigram_11000up.json'))
def ngram_filter(ngram):
    # words can startwith ' and endwith . and interlaced with . and '
    # single -;,: could be anywhere, single .?! only in the end of ngram.
    words_re = r"'?[a-zA-Z]+(['.][a-zA-Z]+)*\.?$"
    end_symbol_re = r"[-;,:.?!]$"
    any_symbol_re = r"[-;,:]$"
    sentence_tag_re = r"</?S>$"
    end_match = partial (re.match, re.compile( '|'.join([words_re, end_symbol_re, sentence_tag_re])))
    any_match = partial (re.match, re.compile( '|'.join([words_re, any_symbol_re, sentence_tag_re])))

    sentence_tag_match = partial(re.match, re.compile( sentence_tag_re ))
    # if not any(sentence_tag_match for s in ngram):
        # return False
    if not all(any_match(s) for s in ngram[:-1]):
        return False
    if not end_match(ngram[-1]):
        return False
    # All words with dot should be less than 6 charactor
    # if any('.' in s and len(s) > 8 for s in ngram):
        # return False
    # at least one word in bnc and wordnet lemmas
    # if not any( s.lower() in lemma_names for s in ngram):
        # return False
    if not all( s in web1t_unigrams_11000up for s in ngram):
        return False
    return True

def mongo_insert(collection, ngram, ngm_count, selector):

        ngm_len = len(ngram)            # 5
        # selector = range( ngm_len )     # [0,1,2,3,4]

        # ngm_count_pack = struct.pack('L', ngm_count) # packed

        # 0 wildcard
        # abc def ghi jkl mno
        rowkey = to_rowkey (ngram, selector)
        column = to_column(ngm_len, selector)

        logging.debug( 'sel_1' + ':\t\t'.join( (str(selector), str(ngram), rowkey, column)) )
        # ngramtableb.put(to_rowkey(ngram, selector, ngm_reversed_count_pack),
                        # {to_column(ngm_len, selector): value})
        collection.insert({"key": rowkey, "position": column, "ngram": ngram, "count": ngm_count})
        


if __name__ == "__main__":
    args = parse_args()
    # import sys
    import pymongo

    mc = pymongo.Connection(args.HOST[0])

    if args.auth:
        mc[args.auth[0]].authenticate(args.auth[1], args.auth[2])

    
    collection = mc[args.DATABASE[0]][args.COLLECTION[0]]

    for line in fileinput.input(args.FILES):
        value = line.strip()
        ngram, ngm_count = line.split('\t')
        ngm_count = int(ngm_count)

        # ngm_reversed_count_pack = '|' +  struct.pack('>Q', (1<<(8*5)) - ngm_count)[3:] # pack (0xFFFFFFFFFF - ngm_count) for reversed sort
                                                                                       # append '|' for hbase scan ENDROW.
        if fileinput.isfirstline():
            logging.info(ngram)

        ngram = ngram.split(' ')        # ['abc', 'def', 'ghi', 'jkl', 'mno']
        if not args.no_filter and not ngram_filter(ngram):
            # logging.info('XXXX {}'.format(ngram))
            print ' '.join(ngram)
            continue

        ngm_len = len(ngram)            # 5
        selector = range( ngm_len )     # [0,1,2,3,4]

        # ngm_count_pack = struct.pack('L', ngm_count) # packed

        # 0 wildcard
        # abc def ghi jkl mno
        # rowkey = to_rowkey (ngram, selector, ngm_reversed_count_pack)
        # column = to_column(ngm_len, selector)
        # 
        # logging.debug( 'sel_1' + ':\t\t'.join( (str(selector), str(ngram), rowkey, column)) )
        # ngramtableb.put(to_rowkey(ngram, selector, ngm_reversed_count_pack),
                        # {to_column(ngm_len, selector): value})
        mongo_insert(collection, ngram, ngm_count, selector)
        
        # batch_cnt += 1
        sel_1, sel_2, sel_3, sel_4 = [None]*4
        # if batch_cnt >= args.batch_count:
            # ngramtableb.send()
            # logging.info('----> send <---- {}'.format(batch_cnt))
            # batch_cnt = 0
        # logging.debug('- no send - {} {}'.format(batch_cnt, args.batch_count))

        # 1 wildcard
        for i in range(ngm_len):
            del sel_1
            sel_1 = copy(selector)
            sel_1.remove(i)
            if len(sel_1) == 0: break

            # rowkey = to_rowkey (ngram, sel_1, ngm_reversed_count_pack)
            # column = to_column(ngm_len, sel_1)
            # logging.debug( 'sel_1' + ':\t\t'.join( (str(sel_1), str(ngram), rowkey, column)) )
            # ngramtableb.put ( rowkey, {column: value })
            # del rowkey, column
            mongo_insert(collection, ngram, ngm_count, sel_1)

            # batch_cnt += 1
            # 2 wildcard
            for j in range(i+1, ngm_len):
                del sel_2
                sel_2 = copy(sel_1)
                sel_2.remove(j)
                if len(sel_2) == 0: break
                # logging.debug('sel_2' + ':\t\t'.join( (str(sel_2), str(ngram), str(to_rowkey(ngram, sel_2, ngm_reversed_count_pack)),to_column(ngm_len, sel_2) )) )
                # rowkey = to_rowkey (ngram, sel_2, ngm_reversed_count_pack)
                # column = to_column(ngm_len, sel_2)
                # ngramtableb.put ( rowkey, {column: value })
                # del rowkey, column
                mongo_insert(collection, ngram, ngm_count, sel_2)

                # batch_cnt += 1
                # 3 wildcard
                for k in range(j+1, ngm_len):
                    del sel_3
                    sel_3 = copy(sel_2)
                    sel_3.remove(k)
                    if len(sel_3) == 0: break

                    # logging.debug( 'sel_3' +':\t\t'.join( (str(sel_3), str(ngram), str(to_rowkey(ngram, sel_3, ngm_reversed_count_pack)),to_column(ngm_len, sel_3))) )

                    # rowkey = to_rowkey (ngram, sel_3, ngm_reversed_count_pack)
                    # column = to_column(ngm_len, sel_3)
                    # ngramtableb.put ( rowkey, {column: value })
                    # del rowkey, column
                    mongo_insert(collection, ngram, ngm_count, sel_3)
                    # batch_cnt += 1
                    # 4 wildcard
                    for l in range(k+1, ngm_len):
                        del sel_4
                        sel_4 = copy(sel_3)
                        sel_4.remove(l)
                        if len(sel_4) == 0: break
                        # logging.debug( 'sel_4' +':\t\t'.join( (str(sel_4), str(ngram), str(to_rowkey(ngram, sel_4, ngm_reversed_count_pack)), to_column(ngm_len, sel_4 )) ) )

                        # rowkey = to_rowkey (ngram, sel_4, ngm_reversed_count_pack)
                        # column = to_column(ngm_len, sel_4)
                        # ngramtableb.put ( rowkey, {column: value })
                        # del rowkey, column
                        mongo_insert(collection, ngram, ngm_count, sel_4)
                        # batch_cnt += 1



    # ngramtableb.send()
    # logging.info('----> send <---- {}'.format(batch_cnt))