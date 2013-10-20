#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# An example on how to create a zimfile using the pyzimlib.

from .zim_writer import ZimArticle, ZimArticleFileBlob, write_zim
import os

### first, go over all files. if html, derive title.
### would relative urls just work?? maybe.


def article_gen_from_tree(topdir):
    for (dirname, _, filenames) in os.walk(topdir):
        for filename in filenames:
            fullname = os.path.join(dirname, filename)
            print(fullname)
            art = ZimArticleFileBlob(filename, blob=fullname)
            yield art 

if __name__ == "__main__":
    articles = article_gen_from_tree(".")
    write_zim(articles,["text/plain",])
