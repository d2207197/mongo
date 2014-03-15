#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
    parser.add_argument('UNIGRAM', nargs=1, help='unigram')
    parser.add_argument('FILES', nargs='*', help='''input files.
    With no FILE, or when FILE is -, read standard input''', default='-')
    # parser.add_argument('-c', '--batch-count', nargs='?', const = 300, action="store", default = 300, type=int, help='Number of rows be sent to hbase per commit. Default is 300')

    parser.add_argument('-n', '--no-filter', action="store_true",  help='Toggle the filter')
    parser.add_argument('-p', '--port',  nargs=1, help='mongodb port', type = int, default = 27017)
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
    # return str(ngm_len) + ':' + ''.join(map(str, sel))
    return ''.join(map(str, sel))


# lemma_names = json.load(open('wordnet_bnc_lemma_names.json'))
web1t_unigrams_11000up = json.load(open('web1t_unigram_11000up.json'))
def ngram_filter(ngram):
    # words can startwith ' and endwith . and interlaced with . and '
    # single -;,: could be anywhere, single .?! only in the end of ngram.
    words_re = r"'?[a-zA-Z]+(['.][a-zA-Z]+)*\.?$"
    end_symbol_re = r"[-;,:.?!]$"
    any_symbol_re = r"[-;,:]$"
    number_re = r"([0-9]+[.,])+[0-9]+$"
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


from bson.binary import Binary
import struct
# def words_to_idxes_binary(uni_collection, words):
#     idxes = [ uni_collection.find_one({'unigram': word}, fields= {'_id': False, 'idx': True})['idx'] for word in words ]
#     return Binary(''.join([ struct.pack('>L', idx)  for idx in idxes ]))

def words_to_idxes_binary( words):
    global uni_idx_map
    idxes = [  uni_idx_map[word] for word in words ]
    return Binary(''.join([ struct.pack('>L', idx)  for idx in idxes ]))


def mongo_insert(collection, uni_collection, ngram, ngm_count, selector):
    ngm_len = len(ngram)            # 5
    rowkey = to_rowkey (ngram, selector)
    rowkey_binary = words_to_idxes_binary(rowkey.split() )
    logging.debug( 'rowkey transform: {} ---> {}' .format( rowkey, rowkey_binary) )

    column = to_column(ngm_len, selector)
    logging.debug( 'sel' + ':\t\t'.join( (str(selector), str(ngram), rowkey, column)) )

    mongo_insert.batch.append({"length": ngm_len, "key": rowkey_binary, "position": column, "ngram": words_to_idxes_binary( ngram), "count": ngm_count })
    if len(mongo_insert.batch) > 5000:
        logging.info( 'insert {} records'.format(len(mongo_insert.batch)))
        collection.insert(mongo_insert.batch)
        del mongo_insert.batch 
        mongo_insert.batch = []

# def ngram_simplifier(ngram):


# {}
if __name__ == "__main__":
    mongo_insert.batch = []
    args = parse_args()
    import sys
    import pymongo

    mc = pymongo.Connection(args.HOST[0], port = args.port)

    if args.auth:
        mc[args.auth[0]].authenticate(args.auth[1], args.auth[2])


    uni_collection = mc[args.DATABASE[0]][args.COLLECTION[0] + '_unigram']
    global uni_idx_map
    uni_idx_map = {}
    idx_uni_map = {}
    collection = mc[args.DATABASE[0]][args.COLLECTION[0]]


    from bson.binary import Binary
    import struct
    for idx, line in enumerate( fileinput.input(args.UNIGRAM[0])):
        unigram, ngm_count = line.strip().split('\t')
        ngm_count = int(ngm_count)
        # binary = Binary(struct.pack('>L', idx) )
        uni_collection.insert({"unigram": unigram, 'idx': idx, 'count': ngm_count})
        uni_idx_map[unigram] = idx
        idx_uni_map[idx] = unigram
    import pickle
    with open('unigram_index.pkl', 'wb') as pickle_file:
        pickle.dump(uni_idx_map, pickle_file)
    with open('index_unigram.pkl', 'wb') as pickle_file:
        pickle.dump(idx_uni_map, pickle_file)

    # sys.exit(0)

    for line in fileinput.input(args.FILES):

        value = line.strip()
        ngram, ngm_count = line.split('\t')
        ngm_count = int(ngm_count)


        if fileinput.isfirstline():
            logging.info('start inserting file: {}'.format(fileinput.filename()))
            logging.info(ngram)

        ngram = ngram.split(' ')        # ['abc', 'def', 'ghi', 'jkl', 'mno']
        if not args.no_filter and not ngram_filter(ngram):
            print ' '.join(ngram)
            continue
        # ngram = ngram_simplifier(ngram)

        ngm_len = len(ngram)            # 5
        selector = range( ngm_len )     # [0,1,2,3,4]

        if not all(word in uni_idx_map for word in ngram ):
            # logging.info('reject {}'.format(ngram))
            continue
        mongo_insert(collection, uni_collection, ngram, ngm_count, selector)
        
        sel_1, sel_2, sel_3, sel_4 = [None]*4
        for i in range(ngm_len):
            del sel_1
            sel_1 = copy(selector)
            sel_1.remove(i)
            if len(sel_1) == 0: break

            mongo_insert(collection, uni_collection, ngram, ngm_count, sel_1)

            for j in range(i+1, ngm_len):
                del sel_2
                sel_2 = copy(sel_1)
                sel_2.remove(j)
                if len(sel_2) == 0: break
                mongo_insert(collection, uni_collection, ngram, ngm_count, sel_2)

                for k in range(j+1, ngm_len):
                    del sel_3
                    sel_3 = copy(sel_2)
                    sel_3.remove(k)
                    if len(sel_3) == 0: break
                    mongo_insert(collection, uni_collection, ngram, ngm_count, sel_3)

                    for l in range(k+1, ngm_len):
                        del sel_4
                        sel_4 = copy(sel_3)
                        sel_4.remove(l)
                        if len(sel_4) == 0: break
                        mongo_insert(collection, uni_collection, ngram, ngm_count, sel_4)
                        
    logging.info( 'insert {} records'.format(len(mongo_insert.batch)))
    collection.insert(mongo_insert.batch)
    del mongo_insert.batch 
    mongo_insert.batch = []





