from __future__ import print_function
from coders import *
import mmh3
import numpy as np
from collections import defaultdict
from array import array
from index import mmh3_hash
from query_parser import Parser
from functools import partial
# from datetime import datetime

def get_postlists(terms, f, coding, n_buckets, buckets_offset, bucket_sizes):
    # print('get_postlists: terms[0] =', terms[0])
    if coding == 'varbyte':
        decode = vb_decode
        code_unit_size = 1
    elif coding == 'simple9':
        decode = partial(s9_decode, remove_zeros=True)
        code_unit_size = 4
    hashes = defaultdict(list)
    hash_idx = defaultdict(list)
    for idx, term in enumerate(terms):
        # if isinstance(term, unicode):
        #     term = term.encode('utf-8')
        mm_hash = mmh3_hash(term)
        hash_idx[mm_hash].append(idx)
        bucket_n = mm_hash & (n_buckets - 1)
        hashes[bucket_n].append(mm_hash)
    ans = [[] for i in xrange(len(terms))]
    if not hashes:
        return ans
    bucket_nums = sorted(hashes.keys())
    max_bucket_n = bucket_nums[-1]
    bucket_locs = np.cumsum([0] + list(bucket_sizes[:max_bucket_n]))
    for bucket_n in bucket_nums:
        f.seek(buckets_offset + bucket_locs[bucket_n])
        query_hashes = set(hashes[bucket_n])
        if not query_hashes:
            continue
        arr = array('I')
        arr.read(f, 1)
        n_hashes = arr[0]
        for i in xrange(n_hashes):
            arr = array('l')
            arr.read(f, 1)
            mm_hash = arr[0]
            arr = array('I')
            arr.read(f, 1)
            pl_size = arr[0]
            if mm_hash in query_hashes:
                query_hashes -= {mm_hash}
                if coding == 'varbyte':
                    arr = array('B')
                    arr.read(f, pl_size)
                    f.seek(f.tell() + (-pl_size) % 4)
                elif coding == 'simple9':
                    arr = array('I')
                    arr.read(f, pl_size / 4)
                pl_deltas = decode(arr)
                for idx in hash_idx[mm_hash]:
                    ans[idx] = list(pl_deltas) # copy
                if not query_hashes:
                    break
            else:
                f.seek(f.tell() + pl_size + (-pl_size) % 4)
    # print('got', np.cumsum(ans))
    return ans

def get_urls(input_docids, f, n_buckets, buckets_offset, bucket_sizes):
    if not len(input_docids):
        return []
    docid_idx = defaultdict(list)
    docids = defaultdict(list)
    for idx, docid in enumerate(input_docids):
        docid_idx[docid].append(idx)
        bucket_n = docid & (n_buckets - 1)
        docids[bucket_n].append(docid)
    ans = [None for i in xrange(len(input_docids))]
    if not docids:
        return ans
    bucket_nums = sorted(docids.keys())
    max_bucket_n = bucket_nums[-1]
    bucket_locs = np.cumsum([0] + list(bucket_sizes[:max_bucket_n]))
    for bucket_n in bucket_nums:
        f.seek(buckets_offset + bucket_locs[bucket_n])
        query_docids = set(docids[bucket_n])
        if not query_docids:
            continue
        arr = array('I')
        arr.read(f, 1)
        n_docids = arr[0]
        for i in xrange(n_docids):
            arr = array('I')
            arr.read(f, 2)
            docid, url_size = arr
            if docid in query_docids:
                query_docids -= {docid}
                arr = array('B')
                arr.read(f, url_size)
                url = arr.tostring()
                for idx in docid_idx[docid]:
                    ans[idx] = url # copy
                if not query_docids:
                    break
            else:
                f.seek(f.tell() + url_size)
    return ans

if __name__ == '__main__':
    with open('index', 'r') as f:
        arr = array('I')
        arr.read(f, 1)
        comp_nbuc = arr[0]
        comp_type = comp_nbuc >> 31
        n_buckets = comp_nbuc & ((1 << 31) - 1)
        if comp_type == 0:
            coding = 'varbyte'
            decode = vb_decode
        elif comp_type == 1:
            coding = 'simple9'
            decode = s9_decode
        bucket_sizes = array('H')
        bucket_sizes.read(f, n_buckets)
        buckets_offset = f.tell()
        postlists_getter = lambda terms: \
            [np.cumsum(postlist) for postlist in get_postlists(
                terms, f, coding, n_buckets, buckets_offset, bucket_sizes
            )]
        parser = Parser(postlists_getter)
        while True:
            try:
                query = raw_input()
                print(query)
            except EOFError:
                break
            # t1 = datetime.now()
            if not query:
                continue
            try:
                parser.parse(query)
            except Exception as e:
                print('PARSING ERROR:', e)
                continue
            parser.prepare_postlists()
            ans = parser.execute()
            assert(sorted(ans) == list(ans))
            # print('ans:', ans)
            print(len(ans))
            if not len(ans):
                # t2 = datetime.now()
                # time_elapsed = (t2-t1).total_seconds()
                # print('nothing found, took {} ms'.format(time_elapsed * 1000.0))
                continue
            with open('doc_ids', 'r') as f_docs:
                arr = array('I')
                arr.read(f_docs, 1)
                n_doc_buckets = arr[0]
                doc_buc_sizes = array('H')
                doc_buc_sizes.read(f_docs, n_doc_buckets)
                doc_buc_offset = f_docs.tell()
                urls = get_urls(ans, f_docs, n_doc_buckets,
                    doc_buc_offset, doc_buc_sizes)
                # t2 = datetime.now()
                for url in urls:
                    print(url)
            # time_elapsed = (t2-t1).total_seconds()
            # print("found {} docs in {} ms".format(len(ans), time_elapsed * 1000.0))
