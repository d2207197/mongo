#!/usr/bin/env/python
'''
This module provides the HBaseNgram class.

HBaseNgram supports linggle query syntax for retrieving ngrams and count from HBase.

To create HBase table for this HBaseNgram:
$ hbase shell
hbase(main):001:0> create 'web1t-ngram-cnt', {NAME => '1', VERSIONS => '1'}, {NAME => '2', VERSIONS => '1'}, {NAME => '3', VERSIONS => '1'}, {NAME => '4', VERSIONS => '1'}, {NAME => '5', VERSIONS => '1'}
'''


from pyparsing import Word, alphas, Literal, operatorPrecedence, opAssoc
from pyparsing import  Optional


from copy import deepcopy
from functools import partial
import logging
reload(logging)
import happybase
from itertools import  islice
from heapq import heapify, heapreplace, heappop
from functools import  total_ordering


LOGGER = logging.getLogger('report')
LOGGER.setLevel(logging.INFO)
FORMATTER = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
CH = logging.StreamHandler()
CH.setFormatter(FORMATTER)
LOGGER.addHandler(CH)



from bson.binary import Binary
import struct



with open('unigram_index.pkl',  'rb') as pickle_file:
    import pickle
    global uni_idx_map
    uni_idx_map = pickle.load(pickle_file)
with open('index_unigram.pkl',  'rb') as pickle_file:
    import pickle
    global idx_uni_map
    idx_uni_map = pickle.load(pickle_file)

def words_to_idxes_binary( words):
    global uni_idx_map
    idxes = [  uni_idx_map[word] for word in words ]
    return Binary(''.join([ struct.pack('>L', idx)  for idx in idxes ]))

def idxes_binary_to_words(binary):
    idxes = [ struct.unpack('>L', binary[i*4:i*4+4])[0] for i in range(len(binary)/4)]
    return [ idx_uni_map[idx] for idx in idxes ]

class MongoKeyPos():
    '''
    Class for construct hbase columns
    '''
    def __init__(self, poss=None, next_pos=0):
        if poss == None:
            poss = []
        self.poss = poss
        self.next_pos = next_pos

    def __deepcopy__(self, memo):
        return MongoKeyPos(deepcopy(self.poss, memo), self.next_pos)
        # elif name == 'next_pos': self[2] = value

    def __str__(self):
        return str(self.next_pos) + ':' + ''.join(map(str, self.poss))


class MongoQuery():
    '''Class for construct a hbase query'''
    def __init__(self, text = None, col=None, filters = None, step=False, append = False):
        if text == None:
            self.text = []
        else:
            self.text = deepcopy(text)

        if col == None:
            self.col = MongoKeyPos()
        else:
            self.col = deepcopy(col)
            
        if filters == None:
            self.filters = []
        else:
            self.filters = deepcopy(filters)


        if step:
            if append:
                col.poss.append(col.next_pos)
            col.next_pos += 1
    

    def __iter__(self):
        yield self.text
        yield self.col
        yield self.filters

    def __deepcopy__(self, memo):
        return MongoQuery(self.text, self.col, self.filters)
        # elif name == 'next_pos': self[2] = value


    def rowkey(self):
        # return ' '.join(self.text)
        return words_to_idxes_binary(self.text)

    def column(self):
        # text_length = str(len(self.text))
        return ''.join(map(str, self.col.poss))
        # return str(self.col)

    def length(self):
        return self.col.next_pos

    def __str__(self):
        return '<\'{}\', {}, {}>'.format(self.rowkey(), self.column(), self.filters)

    def __repr__(self):
        return self.__str__()


class Alternatives():
    '''Class for parsing linggle syntax query'''
    def __init__(self, toks):
        self.alternatives = toks[0][0::2]

    def __str__(self):
        return '(' + '|'.join(map(str, self.alternatives)) + ')'

    def __iter__(self):
        for alt in self.alternatives:
            yield alt


class Query():
    '''Class for parsing linggle syntax query'''

    def __init__(self, toks):
        toks = toks[0][0::2]

        # self._querys = [MongoQuery([], [MongoKeyPos([], 0)])]
        self._querys = [MongoQuery()]

        LOGGER.debug('init Query: {} '.format(self._querys))
        self._maybe = False
        for gram in toks:
            LOGGER.debug('gram: {} {}'.format(str(gram), type(gram)))
            if isinstance(gram, Alternatives):
                LOGGER.debug('-> alternatives')
                if self._maybe:
                    orig_querys = deepcopy(self._querys)
                self._insert_alternatives(gram)
                if self._maybe:
                    self._querys.extend(orig_querys)
            elif gram == '_':
                LOGGER.debug('-> wildcard')
                self._insert_wildcard()
            elif gram == '*':
                LOGGER.debug('-> any wildcard')
                self._insert_any_wildcard()
            elif gram == '?':
                LOGGER.debug('-> maybe')
                self._maybe = True
                continue
            elif gram in ['v.', 'adj.', 'n.', 'adv.', 'det.', 'prep.']:
                LOGGER.debug('-> POS')
                self._insert_POS(gram)
            elif gram == 'STOPHERE':
                return

            else:
                LOGGER.debug('-> word')
                if self._maybe:
                    orig_querys = deepcopy(self._querys)
                self._insert_word(gram)
                if self._maybe:
                    self._querys.extend(orig_querys)
            self._maybe = False
            LOGGER.debug('result: {}'.format(str(self)))

    def _col_step(self, col, append=False ):
        # LOGGER.debug('poss step start: {}'.format(cols))
        # for i, poss in enumerate(cols):
        if col.next_pos + 1 > 5:
            return None
            # to_del.append(i)
            # continue
        if append:
            col.poss.append(col.next_pos)
        col.next_pos += 1
        return col
        # LOGGER.debug('poss: {}'.format(poss))
        # for i in reversed(to_del):
        #     del cols[i]

    def _insert_wildcard(self):
        to_del = []
        if self._maybe:
            orig_querys = deepcopy(self._querys)

        for i, (text, col, filters) in enumerate(self._querys):
                # orig_cols = deepcopy(cols)
            if not self._col_step(col):
                to_del.append(i)
            # if self._maybe:
                # cols.extend(orig_cols)
        for i in reversed(to_del):
            del self._querys[i]
        if self._maybe:
            self._querys.extend(orig_querys)


    def _insert_POS(self, POS):
        to_del = []
        if self._maybe:
            orig_querys = deepcopy(self._querys)

        for i, (text, col, filters) in enumerate(self._querys):
                # orig_cols = deepcopy(cols)
            filters.append((col.next_pos, POS))
            if not self._col_step(col):
                to_del.append(i)
            # if self._maybe:
                # cols.extend(orig_cols)
        for i in reversed(to_del):
            del self._querys[i]
        if self._maybe:
            self._querys.extend(orig_querys)


    def _insert_any_wildcard(self):
        new_querys = []
        for i, (text, col, filters) in enumerate(self._querys):

            new_col = self._col_step(deepcopy(col))
            for offset in range(5 - col.next_pos +1):
                if not new_col:
                    break
                new_querys.append(MongoQuery(text, new_col, filters))
                new_col = self._col_step(deepcopy(new_col))
        self._querys.extend(new_querys)


    def _insert_alternatives(self, alts):
        new_querys = []
        try:
            for i, (text, col, filters) in enumerate(self._querys):
                for alt in alts:
                    new_querys.append(MongoQuery(text + [alt], col=self._col_step(deepcopy(col), append =True), filters = filters))
            del self._querys
            self._querys = new_querys
        except Exception as e:
            print e

    def _insert_word(self, word):
        to_del = []
        for i, (text, col, filters) in enumerate(self._querys):
            if self._col_step(col, append=True):
                text.append(word)
            else:
                to_del.append(i)
        for i in reversed(to_del):
            del self._querys[i]


    def __str__(self):
        return self.__repr__()
    def __iter__(self):
        for q in self._querys:
            yield q

    def __getitem__(self, key):
        return self._querys[key]

    def __repr__(self):
        return 'Query( ' + ', '.join(map(str, self._querys)) + ')'



def queryparser():
    '''generate a linggle query syntax parser
    linggle query syntax -> hbase query'''
    word = Word(alphas + "'" + '.' + ',' + '.' + '<>')
    wildcard, any_wildcard, maybe = Literal('_'), Literal('*'), Literal('?')
    POS = (Literal('adj.')| Literal('n.')|Literal('v.')|Literal('adv.')| Literal('det.')| Literal('prep.'))
    atom = (word | wildcard | any_wildcard | maybe | POS)
    

    query = operatorPrecedence(
        atom,
        [
            ('|', 2, opAssoc.LEFT, Alternatives),
            (Optional('&', default='&'), 2, opAssoc.LEFT, Query)
        ])
    return query




@total_ordering
class Row():
    """
    Class for packing a ngram row

    `Row.ngram`: ngram as list
    `Row.ngram_len`: length of ngram
    `Row.poss`: certain word positions
    `Row.count`: ngram count
    """
    def __init__(self, ngram, ngram_len, positions, count ):
        self.ngram = ngram
        self.ngram_len = ngram_len
        self.positions = positions
        self.count = count
    def __str__(self):
        return 'Row(ngram = {}, ngram_len = {}\
, positions = {}, count = {})'.format(
            str(self.ngram), self.ngram_len, self.positions, self.count)
    def __repr__(self):
        return self.__str__()
    def __iter__ (self):
        yield self.ngram
        yield self.ngram_len
        yield self.positions
        yield self.count

    def __lt__(self, other):
        return self.count > other.count
    def __eq__(self, other):
        return self.count == other.count

    @staticmethod
    def make_from_mongo(row):
        # col, ngram = row[1].items()[0]
        # ngram, count = ngram.split('\t')
        # ngram = ngram.split(' ')
        # count = int(count)
        
        # _, col = col.split(':')
        # ngram_len, poss = col.split('-')
        # poss = map(int, list(poss))

        
        return Row( idxes_binary_to_words(row['ngram']), row['length'], row['position'], row['count'])




# import pymongo
# mc = pymongo.MongoClient('moon.nlpweb.org')
# mc.admin.authenticate('nlplab', 'nlplab634')
import pickle
bncwordlemma = pickle.load(open('bncwordlemma.pick'))

class Monggle:
    def __init__(self, host, database, collection, auth_db = None , user = None, password = None):
        """
        Monggle(host, table)

        return a HBaseNgram object with query() method.

        bnchb = Monggle('hadoop.nlpweb.org', 'ngrams', 'AAN2013')
        result = bnchb.query('play * ?* role', limit = 10)
        for row in result:
             print row
        """
        import pymongo
        mc = pymongo.MongoClient(host)
        if auth_db:
            mc[auth_db].authenticate(user, password)
        self.collection = mc[database][collection]

        # self.host = host
        # self.table = table
        
        # self.pool = happybase.ConnectionPool(size=20, host=host)
        
        # conn = happybase.Connection(host)
        # conn.open()
        # self._table = conn.table(table)

        global bncwordlemma

    def _merge(self, iterables):
        LOGGER.debug('iterables: {}'.format(iterables))
        h = []
        h.append
        for itnum, (it, filters) in enumerate([ (iter(iterable), filters) for iterable, filters in  iterables]):
            try:
                next = it.next
                row = next()
                row = Row.make_from_mongo(row)
                h.append([row, filters, itnum, next])
            except StopIteration:
                pass
        # LOGGER.debug('heap: {}'.format (h))
        heapify(h)
        # LOGGER.debug('heap: {}'.format (h))

        while True:
            try:
                while True:
                    # raises IndexError when h is empty
                    v, filters, itnum, next = s = h[0]
                    # yield self._to_row_for_use(v)
                    row_filter = partial(self._filter, row = v)
                    # print v
                    if all(map(row_filter, filters)):
                        yield v
                    # raises StopIteration when exhausted
                    s[0] = Row.make_from_mongo(next())
                    # restore heap condition
                    heapreplace(h, s)
                    LOGGER.debug('heap: {}'.format (h))
            except StopIteration:
                 # remove empty iterator
                heappop(h)
            except IndexError:
                return
                
    def _filter(self,  flt, row):
        word = row.ngram[flt[0]]
        # try:
        #     POSs = self.mc.BNC.bncwordlemma.aggregate([{"$match": {'word': word}}, {"$unwind": "$lemmas"}, {"$match": {'lemmas.%': {'$gt': 0.3}}}, {'$group': { '_id': '', 'POSs': {'$push': '$lemmas.POS'} }}])['result'][0]['POSs']
        # except IndexError:
        #     return False
        global bncwordlemma
        from operator import itemgetter
        try:
            POSs = map(itemgetter(1), filter ( lambda x: x[2]> 0.3  ,bncwordlemma[word]))
        except KeyError:
            return False
        trans = {'adj.': 'a', 'adv.': 'r', 'v.': 'v', 'n.': 'n', 'det.': 'd', 'prep.': 'p'}
        # print POSs, word, flt, row

        if trans[flt[1]] in POSs:
            return True
        return False

    def _scan(self, query, limit = 0):
        # with self.pool.connection() as conn:

            # conn = happybase.Connection(self.host)
            # conn.open()
            # print 'scanning: {} {}'.format(query.rowkey(), query.column())
            # return self._table.scan(

            # rows = conn.table(self.table).scan(
                # row_prefix=query.rowkey(),
                # columns=[query.column()],
                # limit=limit,
		# batch_size=limit )
            #print 'scanned: {} {}'.format(query.rowkey(), query.column())
        rows = self.collection.find({ 'length': query.length() , 'position': query.column(),'key': query.rowkey()}, fields = ['length', 'position', 'ngram', 'count'], limit = limit).sort('count', -1)

        return rows, query.filters


    def _to_row_for_heap(self,res):
        # LOGGER.debug('res: {}'.format(res))
        row = res[0]
        col, ngram = res[1].items()[0]
        ngram, count = ngram.split('\t')
        ngram = ngram.split(' ')
        count = int(count)
        reversed_count = (1<<(8*5)) - count

        _, col = col.split(':')
        ngram_len, poss = col.split('-')
        poss = map(int, list(poss))
        return reversed_count, count, ngram_len, ngram , poss

    def _to_row_for_use(self,res):
        reversed_count, count, ngram_len, ngram , poss = res
        return Row(ngram, ngram_len, poss, count)


    def query(self, query, limit=None):
        '''
        query(query, limit=None) -> list of Row()

        e.g.
         bnchb = HBaseNgram('hadoop.nlpweb.org', 'bnc-all-cnt-ngram')
         result = bnchb.query('play * ?* role', limit = 10)
         for row in result:
              print row
        '''

        parser = queryparser()
        query += ' STOPHERE'
        querys = parser.parseString(query)[0]
        LOGGER.debug('querys: {}'.format(querys))
        from itertools import imap
        from operator import attrgetter
        if any(imap(len, imap(attrgetter('filters'), querys))):
            limit_timse = 15
        else:
            limit_timse = 1

        limited_scan = partial(self._scan, limit = limit * limit_timse)
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=20) as e:
            # results = e.map(self._scan, querys)
            results = e.map(limited_scan, querys)
            # results =  map (limited_scan, querys)
            # LOGGER.debug('results: {}'.format(results))
            # return list(islice(self._merge(results), limit))
            return list(self._merge(results))
    


if __name__ == '__main__':

    web1t = HBaseNgram('hadoop.nlpweb.org', 'bnc-all-cnt-ngram')
    res = web1t.query('play * ', limit = 2000)
    for row in res:
        print row
    # pdb.set_trace()
    # res = web1t.query('play * ?*', limit = 20)
    # print 'second', list(res)
