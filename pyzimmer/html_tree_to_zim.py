#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# An example on how to create a zimfile using the pyzimlib.

import os
import os.path
import sys

from operator import attrgetter

from .zim_writer import ZimArticle, ZimArticleFileBlob, write_zim

### first, go over all files. if html, derive title.
### would relative urls just work?? maybe.


def article_gen_from_tree(topdir):
    # we have to go through all of the articles in order to sort them by url
    articles = [] # heap?
    for (dirname, _, filenames) in os.walk(topdir):
        for filename in filenames:
            fullname = os.path.join(dirname, filename)
            #print(fullname)
            relname = os.path.relpath(fullname, topdir)
            art = ZimArticleFileBlob(relname, blob=fullname)
            #yield art 
            articles.append(art)
    articles.sort(key=attrgetter('url'))
    return articles


if __name__ == "__main__":
    if len(sys.argv) > 1:
       topdir = sys.argv[1]
    else:
       topdir = "."
    articles = article_gen_from_tree(topdir)
    if os.path.isfile('index.html'):
        mainPage = 'index.html'
    else:
        mainPage = None
    write_zim(articles,["text/plain",], main_page_url=mainPage)
