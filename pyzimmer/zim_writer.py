#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import struct
import lzma
import hashlib
from uuid import uuid4
from io import BytesIO
from operator import attrgetter
from warnings import warn


def write_zim(articles, mimetypes, filename="out.zim", main_page_url=None):
    """ Write a zim file from an article iterator.
    
    Parameters
    ----------
    articles: iterator
        An iterator producing `ZimArticle` instances. Can be a list, 
        iterator or generator. The generator will only be consumed once,
        upon writing. This allows low memory usage.
        It is assumed that the articles are sorted by ns and then url.
    mimetypes: list
        A list of mimetypes. The articles' mimetypes are indices in
        this list.
    filename: string
        The output filename.
    main_page_url: string
        The url of the 'main' or 'welcome' or 'index.html' page
    """
    with open(filename,"wb") as outzim:
        
        zim_header = ZimHeader()
        outzim.seek(zim_header.raw_size())
        
        zim_header.mimeListPos = outzim.tell()
        outzim.write(bytes(ZimMimelist(mimetypes)))
        zim_header.urlPtrPos = outzim.tell()
        
        cluster_grp, dir_entries, titles, urls = split_articles_to_clusters(articles)
        zim_header.clusterCount = len(cluster_grp)
        zim_header.articleCount = len(dir_entries)
        N = zim_header.articleCount

        # add link to mainPage
        if main_page_url is not None:
            try:
                zim_header.mainPage = urls.index(main_page_url.encode('UTF8'))
            except ValueError:
                warn('mainPage url not found [ {0} ]'.format(main_page_url))

        
        ## there must be a better way to find this
        dir_entries_offset = zim_header.urlPtrPos + (12 * N)
        
        outzim.write(dir_entries.table(dir_entries_offset))
        zim_header.titlePtrPos = outzim.tell()
        outzim.write(bytes(ZimTitlePtrTable((titles), N)))
        
        print(dir_entries_offset, outzim.tell())
        assert(dir_entries_offset == outzim.tell())
        outzim.write(dir_entries.data())
        zim_header.clusterPtrPos = outzim.tell()
        
        cluster_offset = zim_header.clusterPtrPos + (8 * zim_header.clusterCount)
        outzim.write(cluster_grp.table(cluster_offset))
        outzim.write(cluster_grp.data())
        zim_header.checksumPos = outzim.tell()
        outzim.seek(0)
        outzim.write(bytes(zim_header))

    with open(filename, "rb") as f:
        m = hashlib.md5()
        m.update(f.read())
        md5 = m.digest()
    with open(filename, "ab") as f:
        f.write(md5)
 
class ZimHeader(object):
    def __init__(self):

        """ """
        # using same names as in the ref
        self.magicNumber = 72173914 
        self.version = 5
        self.uuid =uuid4().bytes_le
        #self.mimeListPos 
        #articleCount
        #clusterCount
        #urlPtrPos
        #titlePtrPos
        #clusterPtrPos
        self.mainPage = int(0xffffffff)
        self.layoutPage = int(0xffffffff)
        #checksumPos
        self.struct = struct.Struct("<II16sIIQQQQIIQ")
    def raw_size(self):
        return self.struct.size
        
    def __bytes__(self):
        
        return self.struct.pack(
            self.magicNumber, self.version,
            self.uuid, self.articleCount, self.clusterCount,
            self.urlPtrPos, self.titlePtrPos, self.clusterPtrPos,
            self.mimeListPos, self.mainPage, self.layoutPage,
            self.checksumPos)
""" """
    
    

def split_articles_to_clusters(articles, max_cluster_size=1e6):
    cluster_grp = DataGroup("temp_clusters.pyzim")
    dir_entries = DataGroup("temp_entries.pyzim")
    titles = []
    urls = []
    
    current_cluster = ZimCluster()
    current_ns = "-"
    for article in articles:
        if ((current_cluster.raw_size() > max_cluster_size) or
            ((article.namespace != current_ns) and len(current_cluster)>0)):
            cluster_grp.append(current_cluster)
            current_cluster = ZimCluster()
        current_ns = article.namespace
        if article.has_blob:
            current_cluster.append(article.read_blob())
            dir_entries.append(article.to_ArticleEntry(len(cluster_grp), 
                                                   len(current_cluster)-1))
        else:
            dir_entries.append(bytes(article))
        title = article.title
        if title != "":
            titles.append(article.title)
        else:
            titles.append(article.url)
        urls.append(article.url)
    cluster_grp.append(current_cluster)
        
    return cluster_grp, dir_entries, titles, urls

class ZimTitlePtrTable(object):
    def __init__(self, titles, N):
        """ titles must be ordered as in url list, NOT alphabetically"""
        title_tuples = sorted(zip(titles, range(N)))
        self.title_idxs = (title_tuple[1] for title_tuple in title_tuples)
        self.N = N
    def __bytes__(self):
        return struct.pack("<"+str(self.N)+"I", *(self.title_idxs))
        
class ZimMimelist(object):
    def __init__(self, mimetypes):
        self.mimetypes = mimetypes
    def __bytes__(self):
        return b"".join(mime.encode("UTF8")+bytes(1) for mime in self.mimetypes)

class ZimLinkTarget(object):
    def __init__(self, url, *, title="", mimetype=0, revision=0, namespace="A"):
        # What appears in article entry:
        self.url = url.encode("UTF8")
        self.title = title.encode("UTF8")
        self.mimetype = mimetype
        self.revision = revision
        self.namespace = bytes(namespace, "ASCII")
        self.parameter_len = 0
        
        #for our usage:
        self.hasblob = False
        self.ns_url = "{}/{}".format(namespace,url)

        self.struct = struct.Struct("<HbcI{0}s{1}s".format(len(self.url)+1, 
                                                 len(self.title)+1))
        
    def __bytes__(self):
        
        return self.struct.pack(self.mimetype, self.parameter_len, 
                           self.namespace, 
                           self.revision, self.url, self.title)

class ZimArticle(ZimLinkTarget):
    def __init__(self, *args, blob, **kwargs):
        super().__init__(*args, **kwargs)
        self.blob = blob
        self.has_blob = True
        self.struct = struct.Struct("<HbcIII{0}s{1}s".format(len(self.url)+1, 
                                                 len(self.title)+1))
        
    def read_blob(self):
        return self.blob
        
    def to_ArticleEntry(self, cluster_number, blob_number):
        print("added article, cluster : {}, blob : {} ".format(cluster_number,blob_number))
        self.cluster_number = cluster_number
        self.blob_number = blob_number
        return bytes(self)
        
    def __bytes__(self):
        
        return self.struct.pack(self.mimetype, self.parameter_len, 
                           self.namespace, 
                           self.revision, self.cluster_number,
                           self.blob_number, self.url, self.title)
                           
class ZimArticleFileBlob(ZimArticle):
    """ here blob is a filename to be read only when writing"""
    def read_blob(self):
        return open(self.blob,"rb").read()
                           
class DataGroup(object):
    """  Collect data objects, and keep an offset list. keep the data objects in a temp file

        used in for direntries and for clusters
    """
    def __init__(self, temp_filename):
        self.relative_offsets = [0]
        self.temp_filename = temp_filename
        self.w_file = open(temp_filename,"wb")

    def append(self, data):
        self.w_file.write(bytes(data))
        self.relative_offsets += [self.w_file.tell()]
    
    def __len__(self):
        """ remember we do not use the last offset """
        return len(self.relative_offsets) - 1
    
    def table(self, initial_offset):
        data_struct = struct.Struct("<" + str(len(self)) + "Q")
        offset_table = (initial_offset + x for x in self.relative_offsets[:-1])
        return data_struct.pack(*offset_table)
        
    def data(self):
        self.w_file.close()
        with open(self.temp_filename, "rb") as f:
            return f.read()


class ZimCluster(object):
    """ A self contained zimcluster. """
    def __init__(self, blobs=[], *, compress=False):
        self.compress = compress
        self.relative_offset_table = [0]
        self.blobs_io = BytesIO()
        for blob in blobs:
            self.append(blob)
            
    def __len__(self):
        return len(self.relative_offset_table) -1 
    
    def raw_size(self):
        return self.blobs_io.tell()
    
    def append(self, blob):
        self.blobs_io.write(bytes(blob))
        self.relative_offset_table += [self.blobs_io.tell()]
    
    def raw_cluster(self):
        table_length = len(self.relative_offset_table)
        first_blob_offset = 4 * table_length
        offset_table = (first_blob_offset + x for x in self.relative_offset_table)
        format_string = "<"+str(table_length)+"I"
        output_table = struct.pack(format_string, *offset_table)
        self.blobs_io.seek(0)
        return output_table + self.blobs_io.read()

    def __bytes__(self):
        if self.compress:
            return bytes((4,)) + lzma.compress(self.raw_cluster())
        else:
            return bytes((1,)) + self.raw_cluster()
    
        
        
