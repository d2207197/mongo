import pymongo
import hashlib
import struct
import fileinput
import logging

import pdb



import json
web1t_unigrams_11000up = json.load(open('web1t_unigram_11000up.json'))
def ngram_filter(ngram):
    from functools import partial
    import re
    global web1t_unigram_11000up


    # words can startwith ' and endwith . and interlaced with . and '
    # single -;,: could be anywhere, single .?! only in the end of ngram.
    words_re = r"'?[a-zA-Z]+(['.][a-zA-Z]+)*\.?$"
    end_symbol_re = r"[-;,:.?!]$"
    any_symbol_re = r"[-;,:.?!]$"
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



def parse_args():
    from argparse import ArgumentParser, RawTextHelpFormatter
    import argparse
    import sys

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

def mongo_put_ngram(collection, ngram, count):
    length = len(ngram)
    #data = dict(zip(['w' + str(n) for n in   range(length)], ngram))
    #data.update({'length': length, 'count': count, 'ngram': ' '.join(ngram)})
    data = {'length': length, 'count': count, 'ngram': ngram}
    collection.insert(data)

if __name__ == "__main__":
    args = parse_args()
    import sys

    mc = pymongo.Connection(args.HOST[0])

    if args.auth:
        mc[args.auth[0]].authenticate(args.auth[1], args.auth[2])

    
    collection = mc[args.DATABASE[0]][args.COLLECTION[0]]

    for line in fileinput.input(args.FILES):
        value = line.strip()
        ngram, ngm_count = line.split('\t')
        ngm_count = int(ngm_count)

        if fileinput.isfirstline():
            logging.info(ngram)

        ngram = ngram.split(' ')        # ['abc', 'def', 'ghi', 'jkl', 'mno']
        if not args.no_filter and not ngram_filter(ngram):
            # logging.info('XXXX {}'.format(ngram))
            #print ' '.join(ngram)
            continue


        mongo_put_ngram(collection, ngram, ngm_count)
