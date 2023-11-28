#!/usr/bin/env python3
# coding: utf-8

# IMPORTS
import bs4 as bs
import configparser
import datetime
import getopt
import inspect
import json
import os
from os import listdir, sep  # , path
from pathlib import Path
import re
import requests
from requests.auth import HTTPBasicAuth
import shutil
import subprocess
import sys
import traceback


PROGRAM_HEADER = """

VERSION: 0.0.1o

Last Update: 20231128
Last Change: S5P Upload Verify O.K.

Changes:
20230801 Initial version
20230814 first valid resto test upload
20230817 used new credentials (own credentials)
20230909 Connected to resto
20230910 egi notebook rewritte test
20230918 minor bugfixing [ reupload, target config ]
20230929 Verify Upload
20231003 Config review, reupload, different maps testing
20231004 minor bug-fixing
20231016 conversion to py, v 0.0.2
20231017 test result edits
20231018 test result edits
20231020 test result edits
20231101 batch test result edits
20231102 batch test result edits
20231107 batch debug test result edits
20231108 batch debug test result edits
20231109 batch debug test result edits
20231114 download core rewritten - due to S3B large files
20231116 new download core, pdb script, S2A ok
20231117 skip cached files, tested S2A, S2B
20231119 align to coding standard advisor
20231128 S5P verify upload O.K. [ continuous download, S5P manifest ]

Description:

DHR1 TO RESTO REWRITTEN: register-stac.sh from DHusTools

Prereqs:

TBD: $ black register-stac.py && flake8 --max-line-length 100

# pip install stactools stactools-sentinel2 stactools-sentinel3 stactools-sentinel5p

TBD:

[*] post request reimplementation

"""

# USE CACHE
USE_CACHE=True

# CHANGE HERE DOWNLOAD DATA DIRECTORY
FDIR_OUT = "/root/dhuspy/register-stac/tmp"
FNAME_LOCK = "register-stac.lock"

# RUNTIME DIR
RUNTIME_DIR = os.getcwd()

# DESTINATION COLLECION TEST PREFIX
DST_COL_TEST_PREFIX = "mp-"

# PLOG MSG TYPES
MTRACE = 3
MDEBUG = 2
MINFO = 1
MWARNING = 0

# INPUT: dhr1 xml files processed by stac tools uploaded to resto catalog
DEBUG = MTRACE  # debug level 3 brings tracebacks, 2 additional messages

# MAX JSON MB PARSE
MAX_JSON_PARSE = 16  # 16 MB
# swap download filename
DOWNLOAD_SWAP_FNAME = "tmp_register_stac.swap"
# PROGRAM EXITS VALUES
P_EXIT_SUCESS = 0
P_EXIT_FAILURE = 1

# DOWNLOAD TIMEOUT
# DOWNLOAD_TIMEOUT = 4096
DOWNLOAD_TIMEOUT = 100


def patch_fdir(FDIR):
    global FDIR_OUT
    if FDIR is None:
        FDIR = FDIR_OUT
    else:
        FDIR = FDIR_OUT + os.sep + FDIR
    if FDIR[-1] != os.sep:
        FDIR = FDIR + os.sep
    return FDIR


# REQ 20230801002 Obtains metadata for the given product from DHuS storage | 003
# read node.xml
def fread(pfile, FDIR=None):
    FDIR = patch_fdir(FDIR)
    with open(FDIR + pfile, "rb") as f:
      txt = f.read()
    plog(f"[*][ fread FDIR: {FDIR} pfile: {pfile}")
    return txt


def fverify(pfile, sz=None, FDIR=None):
    tmp = 0
    ret = None
    #FDIR = patch_fdir(FDIR)
    os.chdir(FDIR)
    try:
        with open(pfile, "r") as f:
          tmp = len(f.read())
          plog(
              f"[+][ File {pfile} verify as txt o.k., size: {str(tmp)} b expected: {str(sz)}]"
          )
        # ret = True
    except Exception as e:
        plog(f"[!][ File {pfile} Verify as txt not o.k. {str(e)}]")
        try:
            with open(pfile, "rb") as f:
              tmp = len(f.read())
              plog(f"[+][ File {pfile} verify as bin o.k., size: {str(tmp)} b]")
            # ret = True
        except Exception as et:
            plog(f"[!][ File {pfile} Verify not o.k. {str(et)}]")
            ret = False
    if sz == tmp:
        plog(f"[*][ File {pfile} download size equals header file size ]")
        ret = True
    return ret


# REQ 20230801002 Obtains metadata for the given product from DHuS storage | 002
# save node.xml
def fwrite(pfile, txt, FDIR=None):
    FDIR = patch_fdir(FDIR)
    # if fverify(pfile, len(txt), "./"):  # 20231116 # clobber off
    #  return 0
    try:
        plog(f"[*] event: fwrite created directory {FDIR}")
        newdir = Path(FDIR)
        # if not newdir.exists():
        newdir.mkdir(parents=True, exist_ok=True)  # 20231019
    except Exception as e:
        plog(f"[!] warning: fwrite create directory {str(e)} {FDIR}")
    try:
        os.chdir(FDIR)
        fpfile = pfile
        if os.sep in pfile:
            fpfile = pfile.split(os.sep)[-1]
        if isinstance(txt, bytes):
            with open(fpfile, "wb") as f:
              f.write(txt)
        else:
            with open(fpfile, "w") as f:
              f.write(txt)
        plog(
          f"[F] written : FDIR: {FDIR} FILE: {pfile} BIN: {str(isinstance(txt, bytes))}"
        )
        fverify(fpfile, len(txt),FDIR)  # 20231116
    except Exception as e:
        plog(f"[*] error: fwrite cannot write {pfile} in {FDIR}")
        plog(f"[*] BIN: {str(isinstance(txt, bytes))} error: {str(e)}")
    return 0


# REQ 20230801002 Obtains metadata for the given product from DHuS storage | 002
# save node.xml
def fexists(file):
    if FDIR_OUT[-1]!=os.sep:
       file=os.sep+file
    str_fname = FDIR_OUT + file
    print(str_fname)
    ret = Path(str_fname).is_file()
    return int(ret)


# inspect
def __FILE__() -> str:
    # ptyhon has a native __file__
    return inspect.currentframe().f_back.f_code.co_filename


def __LINE__() -> int:
    # python has no native __line__, the closest thing I could find was: sys._getframe().f_lineno
    return inspect.currentframe().f_back.f_lineno


# using datetime module
def plog(message, message_priority=1):
    if message_priority > 0 or DEBUG == 1:
        cdt = datetime.datetime.now()
        print(
            "["
            + str(cdt)
            + "]["
            + str(message_priority)
            + "]["
            + inspect.stack()[1].function
            + "]["
            + str(inspect.stack()[1].lineno)
            + "]: "
            + str(message),
            flush=True,
        )


# REQ 20230801003 endpoint specified in configuration - read
# READ CONFIG
def read_ini():
    os.chdir(RUNTIME_DIR)
    config = configparser.ConfigParser()
    config.sections()
    config.read("dhus.ini")
    if "source" not in config and "target" not in config:
        if (
            config["source"]["url"] not in config
            and config["target"]["url"] not in config
        ):
            if (
                config["source"]["url"] not in config
                and config["target"]["url"] not in config
            ):
                if (
                    config["source"]["username"] not in config
                    and config["target"]["username"] not in config
                ):
                    if (
                        config["source"]["password"] not in config
                        and config["target"]["password"] not in config
                    ):
                        plog("[!] Config file read problem")
                        osexit(P_EXIT_FAILURE)
    # plog(str(config.sections()))
    plog("[*] CFG SOURCE URL: " + config["source"]["url"])
    plog("[*] CFG TARGET URL: " + config["target"]["url"])
    # print(config['source']['username'])
    # print(config['source']['password'])
    return config


# TEST
# config=read_ini()


# EXCEPTION HANDLER
def exc_handl(e, msg, warning=True):
    if e is None:
        e = "Undefined Error"
    if DEBUG > 2:
        traceback.print_exc(file=sys.stdout)
    if DEBUG > 1:
        plog(msg, 2)  # CHANGE HERE
    if warning:
        if e is not None:
            plog("[!] Exception message: " + str(e))
    return P_EXIT_FAILURE


def osexit(P_ERR_CODE):
    rmlock()  # 20231116
    exit(P_ERR_CODE)


# REQ 20230801003 endpoint specified in configuration - write
# WRITE CONFIG


def create_ini():
    config = configparser.ConfigParser()
    config["general"] = {"ServerAliveInterval": "45", "Compression": "yes"}
    # SOURCE SERVICE
    config["source"] = {}
    config["source"]["url"] = "dhr1.cesnet.cz"
    # config['source']['username'] = '' # WRITE ONCE READ MANY TIMES
    # config['source']['password'] = '' # WRITE ONCE READ MANY TIMES
    # TARGET SERVICE
    config["target"] = {}
    config["target"]["url"] = "resto-test.c-scale.zcu.cz"
    # config['target']['username'] = '' # WRITE ONCE READ MANY TIMES
    # config['target']['password'] = '' # WRITE ONCE READ MANY TIMES
    with open("dhus.ini", "w") as configfile:
        config.write(configfile)


# create_ini()
# ensure dhu.ini is read only for the owner and set permissions after creation
# $ chmod 400 dhus.ini


# REQ 20230801001 Obtains a product ID from command line attribute - function


def proc_cmd_opts():
    # global ID
    ID = None
    # https://docs.python.org/3/library/getopt.html
    try:
        opts, args = getopt.getopt(sys.argv[1:], [])
        # plog("optlist: "+str(opts))
        # plog("args: "+str(args))
        # plog(len(args))
        if len(args) > 0:
            # plog(args[0])
            ID = args[0]
            plog("[I] INPUT ID: " + ID)
            return ID
        else:
            plog("[ ERR RS-0010 ][!][ NO ARGUMENT SPECIFIED ]")
            osexit(P_EXIT_FAILURE)
    except getopt.GetoptError as e:
        exc_handl(e, "[ ERR RS-0020 ][!][ FAILURE IN ARGUMENTS PROCESSING. ]")
        osexit(P_EXIT_FAILURE)


# BASIC INPUT ID VERIFICATION
def check_source_id(src_id):
    ID = str(src_id)
    FIXED_ID_LEN = 36
    CTRL_001 = 0
    CTRL_002 = 0

    if ID:
        if len(ID) == FIXED_ID_LEN:
            plog("[*] [ CTRL ]: ID LENGTH %d ... O.K.]" % FIXED_ID_LEN)
            CTRL_001 = 1
        try:
            if ID.split("-") == 4:
                CTRL_002 = 1
        except Exception as e:
            exc_handl(
                e, "[ ERR RS-0030 ][!] CTRL_002 DOES NOT COMPLY TO THE COMMON FORMAT"
            )
            # plog(e)
            # plog("[!] CTRL_002 Does not comply to the common format.")
            osexit(P_EXIT_FAILURE)
    if not CTRL_001 == 1 and not CTRL_002 == 1:
        # plog("[ ERR RS-0040 ][!] BOTH CTRL_002 DOES NOT COMPLY TO THE COMMON FORMAT")
        plog(
            "[ERR RS-0040 ][!][ BOTH CTRL_001 OR CTRL_002 FAILED. ID seems to be malformed. ]"
        )
        osexit(P_EXIT_FAILURE)


# TEST
# proc_cmd_opts()


def get_api_large_file(url, basicauth, is_stream):
    local_filename = DOWNLOAD_SWAP_FNAME
    # NOTE the stream=True parameter below
    try:
        with requests.get(
            url=url, auth=basicauth, stream=is_stream, timeout=DOWNLOAD_TIMEOUT
        ) as r:
            r.raise_for_status()
            with open(local_filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    # If you have chunk encoded response uncomment if
                    # and set chunk_size parameter to None.
                    # if chunk:
                    f.write(chunk)
    except Exception as e:
        plog("[!][ Failed to download the file")
        plog("[!][ ERROR: " + str(e))
    return local_filename


# def get_download_link_size(url):
# #site = urllib.urlopen(url)
# #meta = site.info()
# #download_size = meta.getheaders("Content-Length")[0]
# #resp = request.head(url)
# #print(str(resp.headers))
# #plog("[*] expected download size")

#  plog("Too large file, exiting")
#  osexit(0)

#  #site = urllib.urlopen(url)
#  #meta = site.info()
#  #download_size = meta.getheaders("Content-Length")[0]
#  #plog(f"Content-Length: {download_size}")
#  #if download_size > 16*1024*1024:
#  #  plog("Too large file, exiting")
#  #  osexit(0)


def get_download_size(sz, exp_sz):
    if exp_sz != 0:
        pct = int(sz / int(exp_sz) * 100)
        plog(
            f"[I][{pct:03}][ Download Percent: {pct:03}% ][ {sz:12} b / {exp_sz:12} b ]"
        )


def get_progress(cnt, exp_sz):
    if exp_sz != 0:
        blocks = int(cnt * 8192)
        pct = int(blocks / int(exp_sz) * 100)
        plog(
            f"[I][{pct:03}][ Download Percent: {pct:03}% ][ {cnt*8192:12} b / {exp_sz:12} b ]"
        )


def download_file(url, params, basicauth):
    resp = b""
    cnt = 0
    new = True
    exp_sz = 0
    # try:

    # resp = requests.get(url,auth=basicauth,params=params,timeout=DOWNLOAD_TIMEOUT)
    # use requests.get(url, stream=True).headers['Content-length']
    with requests.get(url, params=params, auth=basicauth, stream=True) as r:
        try:
            if new:
                plog(r.headers)
                if "Content-Length" in r.headers:
                    exp_sz = r.headers["Content-Length"]
                    # iexp_sz=int(int(exp_sz)/1024/1024)
                    plog(f"[*] Expected download size: {str(exp_sz)} b ]")
                else:
                    exp_sz = 0
        except Exception as e:
            plog(f"[!] error while expected size retrieval {str(e)}")
        new = False
        r.raise_for_status()  # HERE 20231114
        # with open(fname, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            # If you have chunk encoded response uncomment if
            # and set chunk_size parameter to None.
            if chunk:
                if cnt % 1024 == 0:
                    get_progress(cnt, exp_sz)
                try:
                    if not resp:
                        # resp = chunk.decode("utf-8")  # f.write(chunk) txt
                        resp = chunk  # f.write(chunk) txt
                    else:
                        # resp += chunk.decode("utf-8")  # f.write(chunk) txt
                        resp += chunk  # f.write(chunk) txt
                except Exception as e:
                    if not resp:
                        resp = chunk  # f.write(chunk) bin
                    else:
                        resp += chunk  # f.write(chunk) bin
                    if cnt % 1000 == 0:
                        plog(f"[*][{cnt}][ Download chunk .... {str(e)}]")
                cnt += 1
    # except Exception as e:
    #  plog("[*][ NO MORE DATA ON STREAM ]")
    get_download_size(len(resp), exp_sz)
    return resp  # .decode("utf-8","replace")


# def get_api_head(url):
#    plog("[h] URL: " + url)


#
# def 20231005 revert from rtc try to download data
def get_api(
    hostname,
    sub_url,
    user=None,
    password=None,
    params=dict(),
    post=False,
    is_stream=False,
):
    # VARIABLES
    url = "https://" + hostname + sub_url
    data = None
    resp = None

    plog("[w] URL: " + url)

    try:
        # 20231108
        # GET WEB PAGE HEADER
        basicauth = None
        if user and password:
            basicauth = HTTPBasicAuth(user, password)
        # DOWNLOAD SIZE PREDICTION
        # download_size=0
        # if "headers" in resp:
        #  if "Content-Lenght" in resp.headers:
        #    download_size = resp.headers["Content-Length"][0]
        #    plog("[*][ DOWNLOAD SIZE: "+str(download_size)+" ]")

        # HERE 20231107
        # if "text" not in resp:
        # resp = requests.get(url,auth=basicauth,params=params,timeout=DOWNLOAD_TIMEOUT)
        # rhead = get_url_head(url,auth=basicauth,params=params,timeout=DOWNLOAD_TIMEOUT)
        # resp = resp.text
        # if not resp:
        resp = download_file(url, params, basicauth)
    except Exception as e:
        # ADV DEBUG: plog(resp)
        exc_handl(e, "[!] Cannot download the result page")
    # if resp:
    #  #plog("[O][ RESP[:80]: "+resp[:80])
    #  #plog("[O][ resp len: "+str(len(resp)))
    # PARSE THE JSON
    try:
        # if resp and len(resp.text) < MAX_JSON_PARSE*1024:
        data = resp
        if resp:
            if len(resp) < MAX_JSON_PARSE * 1024:
                # resp=resp.decode("utf-8")
                # data = resp.json() # Check the JSON Response Content documentation below
                # plog(f"[D] str(type(resp)): {str(type(resp))}")
                if isinstance(resp, bytes):
                    resp = resp.decode("utf-8")
                data = json.loads(
                    resp
                )  # Check the JSON Response Content documentation below
    except Exception as e:
        # if DEBUG: print(resp.text)
        plog(f"[!] Cannot parse the json returning the resp {str(e)}")
        # return(data) # 20231109
        # if hasattr(resp,'text'):
        #  return(resp.text)
        # else:
        # return(resp)
    # try:
    #  print(data["title"])
    # except Exception as e:
    #  exc_handl(e,"[!] Cannot get element from the parsed json")
    plog(f"[D] sucess at: {url}")
    return data  # .text


#
# TesT URL ROUTINES
#
def get_collection_metadata():
    config = read_ini()
    server = config["target"]["url"]
    sub_url = "/collections"
    res = get_api(server, sub_url)

    # COLLECTIONS METADATA DEBUG
    plog("[-] VERSION     : " + res["stac_version"])
    plog("[-] ID          : " + res["id"])
    plog("[-] TYPE        : " + res["type"])
    plog("[-] TITLE       : " + res["title"])
    plog("[-] DESCRIPTION : " + res["description"])
    plog("[-] KEYWORDS    : " + str(res["keywords"]))

    # iterate collections
    for i in res["collections"]:
        # ADV DEBUG plog(i)
        plog("[-] ID: " + str(i["id"]) + " [-] TITLE: " + str(i["title"]))
    return res


# source info api print
# res=get_collection_metadata()

# REQ 20230801002 Obtains metadata for the given product from DHuS storage | 001


# Get source metadata
def get_source_metadata(config, P_ID):
    server = config["source"]["url"]
    sub_url = "/odata/v1/Products('" + P_ID + "')/Nodes"
    res = get_api(
        server,
        sub_url,
        user=config["source"]["username"],
        password=config["source"]["password"],
    )  # CONF
    # print(res)
    # fwrite("node.xml",res) # HERE
    # ADV DEBUG
    # plog(res.split('\n')[:10])
    return res


# TEST
# get_source_metadata(config,P_ID)


#
# REQ 20230801002 Obtains metadata for the given product from DHuS storage | 004
# GET SOURCE DHR PRODUCTS
#
# TITLE='xmlstarlet sel -d -T -t -v "//_:entry/_:title" node.xml'
# PREFIX='xmlstarlet sel -d -T -t -v "//_:entry/_:id" node.xml'
#
def update_source_metadata_nodexml(fname, TITLE):
    xml = fread(fname)
    plog(f"[*][ str(xml) update nodexml: {str(xml)[:500]}")
    # plog(xml)
    src_xml = bs.BeautifulSoup(xml, features="xml")
    titles = []
    ids = []
    # for val in src_xml.find_all('entry')[:3]: # LIMITED
    id = ""
    for val in src_xml.find_all("entry"):  # LIMITED
        title = str(val.find("title").get_text())
        titles.append(title)
        plog(title)
        id = str(val.find("id").get_text())
        ids.append(id)
        plog(id)
        break
    if len(titles) > 0:
        plog("TITLE: " + titles[0])
    # if id:
    #  PREFIX=id
    # ID=titles[0]
    # PLATFORM=ID[0:2]
    # plog("PLATFORM: "+PLATFORM)
    # plog("PREFIX: "+PREFIX)
    # PRODUCTURL="/".join(id.split("/")[:-1])
    # plog("PRODUCTURL: "+PRODUCTURL)
    # plog("Platform from title:"
    return titles


# update node.xml TEST
# CID,PLATFORM,titles=update_source_metadata_nodexml("node.xml")
# plog("CID: "+CID + " PLATFORM: " + PLATFORM)


def platform2fname_manifest(P_ID, TITLE, PLATFORM):
    # ADD 20231101
    FNAME_MANIFEST = "manifest.safe"
    # MANIFEST BY PLATFORM
    if PLATFORM == "S1" or PLATFORM == "S2":
        FNAME_MANIFEST = "manifest.safe"
    elif PLATFORM == "S3" or PLATFORM == "S3p":
        FNAME_MANIFEST = "xfdumanifest.xml"
    else:
        # os.rmdir(TITLE)
        FNAME_MANIFEST = TITLE
    return FNAME_MANIFEST


def platform2manifest_url(P_ID, TITLE, PLATFORM, SUFFIX):
    FNAME_MANIFEST = platform2fname_manifest(P_ID, TITLE, PLATFORM)
    sub_url = ()
    # sub_url = (
    #    "/odata/v1/Products('"
    #    + P_ID
    #    + "')/Nodes('"
    #    + TITLE
    #    + "')"
    #    + "/"
    #    + "Nodes('"
    #    + FNAME_MANIFEST
    #    + "')/$value"
    # )
    if PLATFORM == "S1" or PLATFORM == "S2":
        sub_url = (
            "/odata/v1/Products('"
            + P_ID
            + "')/Nodes('"
            + TITLE  # .SUFFIX 20231116
            + "')"
            + "/"
            + "Nodes('"
            + FNAME_MANIFEST
            + "')/$value"
        )
    elif PLATFORM == "S3":
        sub_url = (
            "/odata/v1/Products('"
            + P_ID
            + "')/Nodes('"
            + TITLE  # .SUFFIX 20231116
            + "')"
            + "/"
            + "Nodes('"
            + FNAME_MANIFEST
            + "')/$value"
        )
    # elif PLATFORM == "S3":
    #    sub_url = (
    #        "/odata/v1/Products('"
    #        + P_ID
    #        + "')/Nodes('"
    #        + TITLE
    #        + "."
    #        + SUFFIX
    #        + "')"
    #        + "/"
    #        + "Nodes('"
    #        + FNAME_MANIFEST
    #        + "')/$value"
    #    )
    else:
        # sub_url = "/odata/v1/Products('" + P_ID + "')"
        sub_url = "/odata/v1/Products('" + P_ID + "')/$value"
    return sub_url


# REQ 20230801004 Downloads additional metadata files required by stac-tools
# for the given product type. The actual list of files do download depends on
# product type.
# -> IN VARIABLE: manifest.safe, SAVES: manifest.safe
#
# TBD: xfdumanifest.xml, etc. different handling for different platforms. [ "S1", "S2", "S3", "S3p"
# FOR S1 and S2 get MTD_MSIL2
# /MTD_MSIL2A.xml|MTD_MSIL1C.xml|/MTD_TL.xml|annotation/s1a.*xml"
# sed 's/.*href="//' | sed 's/".*//' |
#
def get_source_metadata_manifest_safe(config, P_ID, TITLE, PLATFORM, SUFFIX="SAFE"):
    FNAME_MANIFEST = platform2fname_manifest(P_ID, TITLE, PLATFORM)
    plog(f"FNAME_MANIFEST {FNAME_MANIFEST}")
    sub_url = platform2manifest_url(P_ID, TITLE, PLATFORM, SUFFIX)
    plog(f"FNAME_URL {sub_url}")
    server = config["source"]["url"]  # REVIEW TBD HERE
    plog(f"SUB_URL: {server}")

    plog(f"[*] manifest to be retrieved from server: {server} url: {sub_url}")
    res = get_api(
        server,
        sub_url,
        user=config["source"]["username"],
        password=config["source"]["password"],
    )  # CONF
    # ADV DEBUG plog(res.split('\n')[:10])
    if res:
        # CREATE DIR IF IT DOES NOT EXISTS
        # try:
        #    os.mkdir(FDIR_OUT + TITLE)
        #    plog(f"[*][001] PRODUCT DIR {TITLE} CREATED.")
        # except Exception as e:
        #    plog(f"[!][001] PRODUCT DIR ALREADY EXISTS {str(e)} " + TITLE)
        # try:
        #    newdir = Path(FDIR_OUT + os.sep + TITLE)
        #    newdir.mkdir(parents=True, exist_ok=True)  # 20231019
        #    plog(f"[*][S002] PRODUCT DIR {TITLE} CREATED.")
        # except Exception as e:
        #    plog(f"[!][S002] PRODUCT DIR ALREADY EXISTS {str(e)} " + TITLE)
        try:
            # fwrite(TITLE+os.sep+FNAME_MANIFEST,res,bin=True)
            fwrite(FNAME_MANIFEST, res, FDIR=TITLE)
            # if SUFFIX != "SAFE":
            #    fwrite(TITLE + "." + SUFFIX + os.sep + FNAME_MANIFEST, res)
            # fwrite(TITLE+os.sep+"manifest.safe",res)
            # fwrite(TITLE+os.sep+"xfdumanifest.xml",res)
            plog(f"[o] PLATFORM: {PLATFORM}, FNAME: {FNAME_MANIFEST} file saved.")
            plog(f"[o] TITLE: {TITLE}{os.sep}{FNAME_MANIFEST} stored.")
        except Exception as e:
            msg = "[*] CANNOT SAVE MANIFEST FILE {FNAME_MANIFEST}"
            exc_handl(e, msg, warning=True)
            osexit(P_EXIT_FAILURE)
    else:
        msg = f"[*] CANNOT SAVE MANIFEST FILE {FNAME_MANIFEST}"
        osexit(P_EXIT_FAILURE)
    # return TITLE + os.sep + FNAME_MANIFEST
    return FNAME_MANIFEST


# TEST
# get_source_metadata_manifest_safe(config,P_ID,TITLE,PLATFORM)


# REQ 20230801004 Downloads additional metadata files required by stac-tools
# for the given product type. The actual list of files do download depends on
# product type.
# -> IN VARIABLE: manifest.safe, SAVES: manifest.safe
# TBD: xfdumanifest.xml, etc. different handling for different platforms. [ "S1", "S2", "S3", "S3p"
# FOR S1 and S2 get MTD_MSIL2
# /MTD_MSIL2A.xml|MTD_MSIL1C.xml|/MTD_TL.xml|annotation/s1a.*xml"
# sed 's/.*href="//' | sed 's/".*//' |
# get_api(hostname,sub_url,params=dict(),user=None,password=None):


def get_product_metadata(config, SRC_PROD_ID):
    # server= "dhr1.cesnet.cz"
    # api_protocol="https://"
    server = config["source"]["url"]  # REVIEW TBD HERE
    sub_url = "/odata/v1/Products('" + SRC_PROD_ID + "')"
    res = get_api(
        server,
        sub_url,
        user=config["source"]["username"],
        password=config["source"]["password"],
    )  # CONF
    # ADV DEBUG
    # plog(res)
    if res:
        fwrite("node.xml", res)
    return res


# TEST
# get_source_metadata_manifest_safe()


# REQ 20230801004 Downloads additional metadata files required by stac-tools
# for the given product type. The actual list of files do download depends on
# product type.
# READS AND PARSES manifest.safe, extracts metadata filenames and paths
# print(ID)
def get_source_metadata_all(ID, TITLE, PLATFORM):
    FNAME_MANIFEST = platform2fname_manifest(ID, TITLE, PLATFORM)
    # UNSAFE # 20231030
    # os.makedirs(FDIR_OUT + TITLE, exist_ok=True)
    # fname_manifest=FNAME_MANIFEST
    plog("[*] MANIFEST READ: " + TITLE + os.sep + FNAME_MANIFEST)
    try:
        mnfst = fread(FNAME_MANIFEST, FDIR=TITLE)
    except Exception as e:
        plog(f"[!][ RS ERR ][ MANIFEST SAFE NOT READY {str(e)}]")
        osexit(P_EXIT_FAILURE)
    # fwrite(ID+os.sep+FNAME_MANIFEST,mnfst)
    src_mnfst = bs.BeautifulSoup(mnfst, features="xml")
    for val in src_mnfst.find_all("entry")[:3]:  # LIMITED
        plog(str(val))
    file_locs = []
    for val in src_mnfst.find_all("fileLocation"):  # NOT LIMITED
        # 20231004 MP added tiff filter
        # GET ONLY METADATA NODES NAMES
        if ".tiff" not in val.get("href"):
            if ".jp2" not in val.get("href"):
                if ".gml" not in val.get("href"):
                    HREF = val.get("href")
                    file_locs.append(HREF)
                    if HREF[:2] == "./":
                        HREF = HREF[2:]  # 20231109
        # FNAME=FDIR_OUT+TITLE+os.sep+(os.sep.join(tmp_href.split(os.sep)))
        plog("href: " + str(HREF))
        # plog("fname: "+str(FNAME))
    # for idx, loc in enumerate(file_locs):
    #  plog("[ "+str(idx)+" ][ "+loc+" ]")
    src_fnames = []
    src_fpaths = []
    for fname in file_locs:
        # print(fname.split('/')[-1])
        src_fnames.append(fname.split("/")[-1])
        src_fpaths.append("/".join(fname.replace("./", "").split("/")[:-1]))
        # src_fpaths.append('/'.join(fname.split('/')[:-1]))
    # ADV DEBUG plog(src_fnames)
    # ADV DEBUG plog(src_paths)
    # for idx, loc in enumerate(src_fnames):
    #  plog("[ "+str(idx)+" ][ "+loc+" ] [ "+src_fpaths[idx]+" ]")
    return (src_fnames, src_fpaths)


# TEST
# get_source_metadata_all(ID)


def get_metadata_file(TITLE, config, urls, src_fpaths, src_fnames):
    src_server = config["source"]["url"]
    for x in range(len(urls)):
        tfname = TITLE + os.sep + src_fpaths[x] + os.sep + src_fnames[x]
        tdir = FDIR_OUT + os.sep + TITLE + os.sep + src_fpaths[x]
        tdirx = TITLE + os.sep + src_fpaths[x]
        # newdir = Path(tdir)
        # newdir.mkdir(parents=True, exist_ok=True)  # 20231019
        # print("[+] mkdir: " + tdir + " ... [ O.K. ]")
        plog("urls [" + str(x) + "]: " + urls[x] + " -> " + tdir)
        # 20231020
        # fverify(tfname, res, tdirx)  # 20231114
        # try to get dest size
        exp_sz = 0
        fdown_sz = 0
        if USE_CACHE is True:
          try:
              tmp_basicauth = None
              user = config["source"]["username"]
              password = config["source"]["password"]
              if user and password:
                  tmp_basicauth = HTTPBasicAuth(user, password)
              tmp_url = "https://" + src_server + urls[x]
              tmp_params = dict()
              with requests.get(
                  tmp_url, params=tmp_params, auth=tmp_basicauth, stream=True
              ) as r:
                  if "Content-Length" in r.headers:
                      exp_sz = int(r.headers["Content-Length"])
                      # plog()
              fdown_sz = len(fread(tfname, FDIR="./"))
          except Exception as e:
              exc_handl(
                  e, f"[*] Expected download size: {str(exp_sz)} b Error: {str(e)} ]"
              )
          plog(f"[I] re-download decision exp_sz: {exp_sz} fdown_sz: {fdown_sz} ")
        # IF NO CACHE IT USES exp_sz AND fdown_sz default to 0
        if exp_sz != fdown_sz:
            # GET THE FILE
            res = get_api(
                src_server,
                urls[x],
                user=config["source"]["username"],
                password=config["source"]["password"],
                is_stream=False,
            )
            if res:
                if isinstance(res, bytes):
                    fwrite(tfname, res, tdirx)  # 20231108 # handle binary files
                    # fwrite(tfname, res, bin=True)  # 20231108 # handle binary files
                else:
                    fwrite(tfname, res, tdirx)  # 20231114
                print("[v] Download: " + urls[x] + " ... [ O.K. ]")
            else:
                print("[!] failed to download: " + urls[x] + " ... [ X ]")
        else:
            print(f"[C] File Download skip (cached) File: {tfname} Size: {fdown_sz}")


# PATIENCE (takes cca 10 secs., opt. candidate)
def metadata_json_patch(config, server, src_fnames, src_paths, PROD_ID, NODE_NAME):
    urls = []
    for x in range(len(src_fnames)):
        subprod = ""
        if len(src_paths[x]) > 1:
            elem = src_paths[x] + "/" + src_fnames[x]
        else:
            elem = src_fnames[x]
        if elem[-1] == "/":
            elem[:-1]  # UGLY UGLY UGLY
        # subprod+="/Nodes('"+elem+"')"
        subprod = "/Nodes('" + elem.replace("/", "')/Nodes('") + "')"

        plog("MJPE: " + elem)
        # url = "/odata/v1/Products('"+PROD_ID+"')/Nodes('"+NODE_NAME+"')"+subprod+"" # 20231020
        url = (
            "/odata/v1/Products('"
            + PROD_ID
            + "')/Nodes('"
            + NODE_NAME
            + "')"
            + subprod
            + "/$value"
        )
        # plog(str(elem)+" -> "+urls[x]+" -> "+src_fnames[x])
        # plog(url)
        urls.append(url)
    #
    # plog(src_fnames)
    # plog(urls)
    #
    return (urls, src_paths, src_fnames)


# TEST
# src_server=server
# metadata_json_patch(config,src_server,src_fnames,src_paths),P_ID,SID

# REQ 20230801004 Determines which collection the product belongs to
# MAPS SOURCE PRODUCT NAMES TO TARGET NAMES COLLECTIONS


def translate_prod2col(titles, PLATFORM, test_col_prefix=DST_COL_TEST_PREFIX):
    # TEST ONLY
    # test_col_prefix="mp-"

    rearr = [
        ["^S1[A-DP]_.._GRD[HM]_.*", "sentinel-1-grd"],
        ["^S1[A-DP]_.._SLC__.*", "sentinel-1-slc"],
        ["^S1[A-DP]_.._RAW__.*", "sentinel-1-raw"],
        ["^S1[A-DP]_.._OCN__.*", "sentinel-1-ocn"],
        ["^S2[A-DP]_MSIL1B_.*", "sentinel-2-l1b"],
        ["^S2[A-DP]_MSIL1C_.*", "sentinel-2-l1c"],
        ["^S2[A-DP]_MSIL2A_.*", "sentinel-2-l2a"],
        ["^S3[A-DP]_OL_1_.*", "sentinel-3-olci-l1b"],
        ["^S3[A-DP]_OL_2_.*", "sentinel-3-olci-l2"],
        ["^S3[A-DP]_SL_1_.*", "sentinel-3-slstr-l1b"],
        ["^S3[A-DP]_SL_2_.*", "sentinel-3-slstr-l2"],
        ["^S3[A-DP]_SR_1_.*", "sentinel-3-stm-l1"],
        ["^S3[A-DP]_SR_2_.*", "sentinel-3-stm-l2"],
        ["^S3[A-DP]_SY_1_.*", "sentinel-3-syn-l1"],
        ["^S3[A-DP]_SY_2_.*", "sentinel-3-syn-l2"],
        ["^S5[A-DP]_OFFL_L1_.*", "sentinel-5p-l1"],
        ["^S5[A-DP]_NRTI_L1_.*", "sentinel-5p-l1"],
        ["^S5[A-DP]_OFFL_L2_.*", "sentinel-5p-l2"],
        ["^S5[A-DP]_NRTI_L2_.*", "sentinel-5p-l2"],
    ]

    res_title = None

    for x in titles:
        # print(re.sub('^S2[A-DP]_MSIL2A_.*','sentinel-2-l2a',x))
        for rule in rearr:
            res_title = re.sub(rule[0], rule[1], x)
            if res_title:
                if x != res_title:
                    plog(str(x) + " -> " + test_col_prefix + res_title)
                    break
    return test_col_prefix + res_title
    # s/^S2[A-DP]_MSIL2A_.*/sentinel-2-l2a/


# REQ 20230801005 Runs stac-tools to generate a STAC Item description for the product | 001
def cmd_stac(params):
    result = subprocess.run(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # plog("RES: "+str(result))
    return result


# REQ 20230801005 Runs stac-tools to generate a STAC Item description for the product | 002
# ERR: FileNotFoundError: [Errno 2] No such file or directory:
# TBD: COMPARE register-stac.sh downloaded metadata files to this script downloaded metadata
# ERR: stac-tools returns: SyntaxError: prefix 'n1' not found in prefix map
# WARNINGS: FixWindingWarning: The exterior ring of this shape is wound clockwise.
# /mnt/sdb1/DHusTools/tmp/mp-sentinel-2-l2a/metadata.xml
def run_stac_tools(STAC_BIN, platform, title, SRC_DIR="./"):
    res=None
    plog("[I] TITLE TO RUN STAC TOOLS: " + str(title))
    # TBD: Explore windingw no fix
    params = []
    # 20230921
    if platform == "S1":
        params = [STAC_BIN, "sentinel1", "grd", "create-item", title, SRC_DIR]
    elif platform == "S2":
        params = [STAC_BIN, "sentinel2", "create-item", title, SRC_DIR]
    elif platform == "S3":
        params = [STAC_BIN, "sentinel3", "create-item", title, SRC_DIR]
    elif platform == "S5":
        params = [STAC_BIN, "sentinel5p", "create-item", title, SRC_DIR]
    plog("CALL STAC: " + str(" ".join(params)))
    # cmdres=cmd_stac(['ls','-l'])
    cmdres = cmd_stac(params)
    if cmdres.returncode == 0:
        for stdres in cmdres.stdout:
            plog(stdres)
        res=0
    else:
        for stdres in cmdres.stderr.decode().split("\n"):
            plog(stdres)
        res=100
    return (cmdres.stdout,res)
    ################################


# TBD: different sentinels - VERIFY 20230921
# TEST
# run_stac_tools(PLATFORM,STAC_BIN,TITLE)

# if [ "$PLATFORM" == "S2" ]; then
# 	~/.local/bin/stac sentinel2 create-item "${TITLE}" ./
# elif [ "$PLATFORM" == "S1" ]; then
# 	~/.local/bin/stac sentinel1 grd create-item "${TITLE}" ./
# elif [ "$PLATFORM" == "S3" ]; then
# 	~/.local/bin/stac sentinel3 create-item "${TITLE}" ./
# elif [ "$PLATFORM" == "S5" ]; then
# 	~/.local/bin/stac sentinel5p create-item "${TITLE}" ./
# fi


# REQ 20230801006 Modifies asset URLs to match actual URLs in DHuS.


# TBD: GET THE NEWEST json IN THE GIVEN DIR
def ls(path):
    f = []
    try:
        for fname in listdir(path):
            f.append(path + sep + fname)
        f.sort()
    except Exception as e:
        f = ["[ " + str(e) + "]"]
    return f


# REQ 20230801006 Modifies asset URLs to match actual URLs in DHuS.
# find first json file [ almost random TBD: algoritmize precisiation ]
# and use it in next processing
def get_json_ls(idir=".", PLATFORM="S2A"):
    dls = ls(idir)
    fname = None
    for fname in dls:
        if "json" in fname and PLATFORM in fname:  # 20230919 HERE S2A to S2
            return fname


# TEST
# fname=get_json_ls()
# print(fname)


# REQ 20230801006 Modifies asset URLs to match actual URLs in DHuS.
def update_json_hrefs(HOST, P_ID, fname, fname_out="resto-test_upload.json"):
    debug_test_href = ""
    fcjson = fread(fname)
    djson = json.loads(fcjson)
    # print(djson)
    orig_json = djson
    djson = orig_json
    upload_json = orig_json
    # json_id=''.join(AID.split('.')[:-1])
    json_id = "dhr1" + P_ID  # TBD REVIEW HERE
    upload_json["id"] = json_id
    for key in djson.keys():
        if key == "assets":  # ASSETS DRILL DOWN
            try:
                # print(str(key))
                for x in djson[key]:  # FOR EACH ASSETS
                    # print(x)
                    # for l in x: # SEEK FOR HREF
                    href = djson[key][x].get("href")  # GET HREF
                    if href:  # IF HREF
                        # print(href)
                        url = ""
                        new_href = "Nodes('" + href.replace("/", "')/Nodes('") + "')"
                        if href[-1] == "/":
                            href = href[:-1]  # TBD: PREFFITY [ UGLY UGLY UGLY ]
                        # url=HOST+"odata/v1/Products('"+P_ID+"')/"+new_href+"/$value"
                        # url=HOST+"odata/v1/Products('"+P_ID+"')/"+new_href+"/$value"
                        url = (
                            HOST + "odata/v1/Products('" + P_ID + "')/" + new_href
                        )  # 20231019
                        upload_json[key][x]["href"] = url
                        if debug_test_href == "":
                            # print(upload_json[key][x]["href"])
                            debug_test_href = upload_json[key][x]["href"]
            except Exception as e:
                plog(f"[*][ djson.keys() iteration excpetion pass {str(e)}]")
                pass
    plog("res: " + str(upload_json)[:100] + " ...")
    plog("res href: " + debug_test_href)
    upload_json = djson
    djson = orig_json
    fwrite(fname_out, json.dumps(upload_json), "./")
    # print(upload_json)
    return fname_out, upload_json


# TEST
# fname_out="resto-test_upload.json"
# update_json_hrefs(fname,fname_out)
# fwrite("resto-test_upload.json",json.dumps(upload_json))


# TBD: BASIC JSON CHECKS


# REQ 20230801008 ploads the resulting STAC Item into the catalogue (endpoint in configuration)
# curl -n -o output.json -X POST "${STACHOST}/collections/${COLLECTION[${PLATFORM}]}/items"
# -H 'Content-Type: application/json' -H     'Accept: application/json'
# --upload-file "new_${file}"
# COM 20231109
# def view_col_items(DST_COLLECCTION,DST_URL):
#  #basicauth=None
#  #resto_url="resto-test.c-scale.zcu.cz"
#  server_protocol="https://"
#  server=config["target"]["url"] # REVIEW TBD HERE
#  #sub_url="/collections"+DST_COLLECTION+"["+PLATFORM+"]"+"/"+"items"
#  sub_url="/collections" # /"+DST_COLLECTION+"["+PLATFORM+"]"
#  sub_url="/collections/"+DST_COLLECTION+"/"+"items" # ?
#  #sub_url="/collections/"+DST_COLLECTION+"/"+"items"
#  #ruser=fread("resto_user.txt").strip()
#  #rpass=fread("resto_pass.txt").strip()
#  ruser=config["target"]["username"].strip() # REVIEW TBD HERE
#  rpass=config["target"]["password"].strip() # REVIEW TBD HERE
#  # print(ruser+" "+rpass) # DEBUG
#  basicauth=HTTPBasicAuth(ruser, rpass)
#  resp = requests.get(server_protocol+DST_URL+sub_url,auth=basicauth)
#  #print(resp.text) # DEBUG
#  data=json.loads(resp.text)
#  #print(data["title"])
#  return(data)


# TBD: TRANSLATE COLLECTIO NAME FROM ID
# TBD: VERIFY THE UPLOADED JSON
# curl -n -o output.json -X POST "${STACHOST}/collections/${COLLECTION[${PLATFORM}]}/items"
# -H 'Content-Type: application/json' -H     'Accept: application/json'
# --upload-file "new_${file}"


def upload_collection(config, fname_out, STAC_COL, PLATFORM):
    # basicauth=None
    resto_url = "resto-test.c-scale.zcu.cz"
    resto_url = config["target"]["url"]  # REVIEW TBD HERE
    # sub_url="/collections/"+STAC_COL+"["+PLATFORM+"]"+"/"+"items"
    sub_url = "/collections/" + STAC_COL + "/" + "items"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    # ruser=config['target']['username'].strip()
    # rpass=config['target']['password'].strip()
    ruser = config["target"]["username"].strip()  # REVIEW TBD HERE
    rpass = config["target"]["password"].strip()  # REVIEW TBD HERE
    # print(ruser+" "+rpass) # DEBUG
    basicauth = HTTPBasicAuth(ruser, rpass)
    plog(resto_url + sub_url)
    # noway
    # files = {'file': open('tmp.json','rb')}
    # open(path_file,'r').read()
    json_data = fread(fname_out)
    # files={'file': fobj})
    resp = requests.post(
        "https://" + resto_url + sub_url,
        headers=headers,
        data=json_data,
        auth=basicauth,
    )
    # print(resp.text) # DEBUG
    data = json.loads(resp.text)
    # print(data["title"])
    # print(data)
    return data


# TBD: test reupload
# upload_collection()


# curl -n -o output.json -X POST "${STACHOST}/collections/${COLLECTION[${PLATFORM}]}/items"
# -H 'Content-Type: application/json' -H     'Accept: application/json'
# --upload-file "new_${file}"


def test_resto_api(config):
    # basicauth=None
    data = None
    server_protocol = "https://"
    # resto_url="resto-test.c-scale.zcu.cz"
    resto_url = config["target"]["url"]  # REVIEW TBD HERE
    sub_url = ""
    headers = {"Content-Type": "application/json; charset=utf-8"}
    # ruser=fread("resto_user.txt").strip()
    # rpass=fread("resto_pass.txt").strip()
    # ruser=config['target']['username'].strip()
    # rpass=config['target']['password'].strip()
    ruser = config["target"]["username"].strip()  # REVIEW TBD HERE
    rpass = config["target"]["password"].strip()  # REVIEW TBD HERE
    # print(ruser+" "+rpass) # DEBUG
    basicauth = HTTPBasicAuth(ruser, rpass)
    plog(resto_url + sub_url)
    resp = requests.get(
        server_protocol + resto_url + sub_url, headers=headers, auth=basicauth
    )
    # print(resp.text) # DEBUG
    # noway
    # files = {'file': open('tmp.json','rb')}
    # open(path_file,'r').read()
    # json_data=fread(fname_out)
    # files={'file': fobj})

    data = json.loads(resp.text)
    # print(data["title"])
    # print(data)
    return data


# TEST URL ROUTINES


def test_target_url(config):
    # server= "resto-test.c-scale.zcu.cz"
    server = config["target"]["url"]
    sub_url = "/collections"
    res = get_api(server, sub_url)

    # COLLECTIONS METADATA
    plog("[-] VERSION     : " + res["stac_version"])
    plog("[-] ID          : " + res["id"])
    plog("[-] TYPE        : " + res["type"])
    plog("[-] TITLE       : " + res["title"])
    plog("[-] DESCRIPTION : " + res["description"])
    plog("[-] KEYWORDS    : " + str(res["keywords"]))

    # iterate collections
    for i in res["collections"]:
        plog("[-] ID: " + str(i["id"]) + " [-] TITLE: " + str(i["title"]))
    return res


#  GET PRODUCT NAME BY ID
def get_product_name_by_id(pro_meta, HOST):
    SID = None
    try:
        pro_meta_arr = bs.BeautifulSoup(pro_meta, features="xml")
        for val in pro_meta_arr.find_all("properties"):
            for v in val.find_all("Name"):
                plog("PROD NAME: " + v.get_text())
                SID = v.get_text()
        return SID
    except Exception as e:
        plog(e)
        plog("[!] Cannot read product from " + HOST)
        osexit(P_EXIT_FAILURE)


def rmlock():
    try:
        dec_rm = fexists(FNAME_LOCK)  # TEST IF THE FILE EXISTS
        if dec_rm: 
          os.remove(FDIR_OUT +os.sep + FNAME_LOCK)
    except Exception as e:
        plog(f"[ ERR RS-0010 ][!][ CANNOT REMOVE THE LOCK FILE ERR: {str(e)}]")


# source info api print
# res=test_target_url(config)


def get_gsm(config, SRC_PROD_ID, TITLE):
    fnodexml = "node.xml"
    # download node.xml from source
    gsm = get_source_metadata(config, SRC_PROD_ID)
    # plog("GSM: "+gsm)
    plog("[I] GSM DOWNLOADED")
    if gsm:
        # fnodexml=os.sep+TITLE+os.sep+"node.xml" # n1 bug at the stac runtime
        fwrite(fnodexml, str(gsm), TITLE)  # HERE
    else:
        plog("Get Source Metadata Returns No Data.")
    # plog(fnodexml)
    plog("GSM DOWNLOADED :" + str(gsm))
    plog("[*] GSM DOWNLOADED")
    return fnodexml, gsm


def get_suffix_from_titles(titles):
    SUFFIX = "SAFE"
    if titles:
        if len(titles) > 0:
            if "." in titles[0]:
                NSUFFIX = titles[0].split(".")[1]
                if NSUFFIX != "SAFE":
                    plog("[*] Other suffix than SAFE")
                    SUFFIX = NSUFFIX
    return SUFFIX


# 20231117 # get the product id soonest
def get_src_prod_id_from_gsm(gsm):
  src_xml = bs.BeautifulSoup(gsm, features="xml")
  src_prod_id = ""
  for val in src_xml.find_all("entry"):  # LIMITED
      src_prod_id = str(val.find("Name").get_text())
  #plog(f"src_prod_id: {src_prod_id}")
  return(src_prod_id)
 

####################################################################
#
# RUNTIME
#
####################################################################
def main():
    #
    # MAIN RUNTIME
    #
    #
    # MAIN ERROR RETURN CODE
    #
    P_EXIT_FAILURE = 1

    #
    # INITIALIZATION ROUTINES [ lock file test and write ]
    #
    dec_proc_run = fexists(FNAME_LOCK)  # TEST IF THE FILE EXISTS
    if dec_proc_run == 1:
        plog(f"[*][ register-stack.lock file present in {FDIR_OUT} ]")
        osexit(P_EXIT_FAILURE)

    #
    # GET CURRENT PROCESS ID
    #
    try:  # OS DEPENDEND
        pid = os.getpid()
    except Exception as e:
        plog(f"[*][ CURRENT PROCESS OPERATING SYSTEM ID: {str(pid)} ERR: {str(e)}")

    #
    # WRITE DOWN THE LOCK
    #
    try:  # OS DEPENDEND
        fwrite(FNAME_LOCK, str(pid))  # WRITE DOWN THE LOCK FILE
    except Exception as e:
        plog(f"[*][ CANNOT START THE PROGRAM. {str(e)}")

    # MAIN PROGRAM TRY
    try:
        #
        # VARIABLES
        #
        SRC_PROD_ID = None

        # FILE VARIABLES
        INP_PROD_ID = proc_cmd_opts()
        check_source_id(INP_PROD_ID)
        # READ THE CONFIGURATION
        config = read_ini()
        home_folder = os.getenv("HOME")
        plog(f"[*][ RETRIEVED HOME FOLDER PATH {home_folder}]")

        # STAC BINARY LOCATION
        STAC_BIN = "/usr/local/bin/stac"
        SRC_URL = config["source"]["url"]
        DST_URL = config["target"]["url"]
        sub_url = "/collections"
        SRC_PROD_NAME = None

        # Retrieve product metadata from source
        pro_meta = get_product_metadata(config, INP_PROD_ID)
        # 20231116 # get the product id soonest
        # src_xml = bs.BeautifulSoup(pro_meta, features="xml")
        # for val in src_xml.find_all("entry"):  # LIMITED
        #    src_prod_id = str(val.find("Name").get_text())
        # plog(f"src_prod_id: {src_prod_id}")

        SRC_PROD_NAME = get_product_name_by_id(pro_meta, SRC_URL)
        PLATFORM = SRC_PROD_NAME[:2]
        # if PLATFORM!="S5":
        #  fwrite(SRC_PROD_NAME, pro_meta)  # HERE
        # SRC_PROD_NAME = SRC_PROD_NAME + ".SAFE"  # 20231116 thx zsustr
        fnodexml, gsm = get_gsm(config, INP_PROD_ID, SRC_PROD_NAME + ".SAFE")
        # 20231116 # get the product id soonest
        src_prod_id=get_src_prod_id_from_gsm(gsm)
        #src_xml = bs.BeautifulSoup(gsm, features="xml")
        #src_prod_id = ""
        #for val in src_xml.find_all("entry"):  # LIMITED
        #    src_prod_id = str(val.find("Name").get_text())
        plog(f"src_prod_id: {src_prod_id}")
        # 20231116
        # fwrite(SRC_PROD_ID, pro_meta)  # HERE 20231116

        DST_COLLECTION = None
        SRC_PROD_ID = src_prod_id
        # DEBUG RUNTIME CHECK
        plog("[S] SOURCE PRODUCT ID   : " + SRC_PROD_ID)
        plog("[S] SOURCE PRODUCT NAME : " + SRC_PROD_NAME)
        plog("[S] SOURCE USER         : " + config["source"]["username"])
        plog("[S] SOURCE HOST         : " + SRC_URL)
        plog("[T] TARGET USER         : " + config["target"]["username"])
        plog("[T] TARGET HOST         : " + DST_URL)
        # plog("[I] TITLE               : " + TITLE) # 20231116
        plog("[I] PLATFORM            : " + PLATFORM)
        #
        # TEST SOURCE AND TARGET AVAILABILITY [ TESTS ONLY? 20231030 ]
        #
        plog("[0] EVENT: getting source metadata manifest safe")
        DST_COLLECTION = translate_prod2col([SRC_PROD_ID], PLATFORM, DST_COL_TEST_PREFIX)
        #plog("DST_COLLECTION: " + DST_COLLECTION)        # 20231117
        DST_FILE=f"{FDIR_OUT}{os.sep}{SRC_PROD_ID}"
        if PLATFORM == "S5":
          SRC_DIR=f"{SRC_PROD_ID}"
          TMP_DIR=f"{SRC_PROD_ID}_tmp{os.sep}"
          SRC_FILE=f"{TMP_DIR}{SRC_PROD_ID}"
          #DST_FILE=f"{SRC_PROD_ID}{os.sep}{SRC_PROD_ID}"
          #FILE_TEST=fexists(DST_FILE)
          TRG_FNAME=f"{SRC_PROD_ID}"
          DST_FNAME=f"{SRC_PROD_ID}{os.sep}{SRC_PROD_ID}"
          FILE_TEST=fexists(DST_FNAME)
          TRG_TEST=fexists(TRG_FNAME)
          plog(f"[*] FILE_TEST {str(FILE_TEST)} {DST_FNAME}")
          if TRG_TEST == 0:  # TEST IF THE FILE EXISTS
            try:
              shutil.move(FDIR_OUT+os.sep+SRC_DIR,FDIR_OUT+os.sep+TMP_DIR)
              plog(f"[*] mv {SRC_DIR} {TMP_DIR}")
            except Exception as e:
              exc_handl(e, "[ ERR RS-1201 ][!][ FAILURE IN MAIN. ]")
              plog(f"[ S5 ERROR ][ {str(e)} ]")
            try:
              shutil.move(FDIR_OUT+os.sep+SRC_FILE,FDIR_OUT+os.sep+TRG_FNAME)
              plog(f"[*] mv {SRC_FILE} {DST_FNAME}")
            except Exception as e:
              exc_handl(e, "[ ERR RS-1202 ][!][ FAILURE IN MAIN. ]")
              plog(f"[ S5 ERROR ][ {str(e)} ]")
            try:
              os.rmdir(FDIR_OUT+os.sep+TMP_DIR)
              plog(f"[*] rmdir {TMP_DIR}")
            except Exception as e:
              exc_handl(e, "[ ERR RS-1203 ][!][ FAILURE IN MAIN. ]")
              plog(f"[ S5 ERROR ][ {str(e)} ]")
          plog(f"[*] FILE_TEST {TRG_TEST} {fexists(TRG_FNAME)} {str(TRG_FNAME)}")
          TRG_TEST=fexists(TRG_FNAME)
          if TRG_TEST == 0:
            fname_manifest = get_source_metadata_manifest_safe(
              config, INP_PROD_ID, SRC_PROD_ID, PLATFORM
            )
            plog(f"[0] EVENT: has source metadata manifest safe {fname_manifest}")
        #plog(f"[0] EVENT: has source metadata manifest safe {fname_manifest}")
        elif PLATFORM == "S1" or PLATFORM == "S2":

          metadata = get_source_metadata_all(SRC_PROD_ID, SRC_PROD_ID, PLATFORM)
          plog(f"[I] metadata type: {str(type(metadata))}")
          fnodexml, gsm = get_gsm(config, INP_PROD_ID, SRC_PROD_ID)
          titles = update_source_metadata_nodexml(fnodexml, SRC_PROD_NAME)
          SUFFIX = get_suffix_from_titles(titles)
          plog(f"[*] titles: {str(titles)} SUFFIX: {SUFFIX}")
          plog("[*] SRC_PROD_ID: " + SRC_PROD_ID)
          # PLACEHOLDER FOR FIXED PROC_CMD_OPTS
          # Get the manifest.safe
          # GET ALL SOURCE METADATA - only for given sentinels
          # if PLATFORM == "S1" or PLATFORM == "S2":
          # Update node.xml source metadata

          # ADV DEBUG
          # plog("P_ID: " + SRC_PROD_ID)
          # plog("titles: " + str(titles))
          #
          plog(
              f"[*] titles {titles} PLATFORM {PLATFORM} COL_PREFIX {DST_COL_TEST_PREFIX}"
          )

          #
          # GET DESTIONATION COLLECTION ID FROM PRODUCT ID
          # 20231128
          # DST_COLLECTION = translate_prod2col(titles, PLATFORM, DST_COL_TEST_PREFIX)
          # plog("DST_COLLECTION: " + DST_COLLECTION)

          src_fnames, src_paths = get_source_metadata_all(
              SRC_PROD_ID, SRC_PROD_ID, PLATFORM
          )
          # ADV DEBUG
          # plog(f"src_fnames: {src_fnames[:3]}")
          # plog(f"src_paths: {src_paths[:3]}")

          #
          # Patch the metadata
          #
          urls2, src_fpaths2, src_fnames2 = metadata_json_patch(
              config, SRC_URL, src_fnames, src_paths, INP_PROD_ID, SRC_PROD_ID
          )

          plog("URLS2: " + str(urls2))
          # These are the larger downloads
          get_metadata_file(SRC_PROD_ID, config, urls2, src_fpaths2, src_fnames2)

          # ADV DEBUG
          # plog("src_fnames[:3] " + str(src_fnames[:3]))
          # plog("src_fpaths[:3] " + str(src_fpaths2[:3]))
          # plog("urls2[:3] " + str(urls2[:3]))

          # if titles:
          #    if len(titles) > 0:
          #        SRC_PROD_NAME = titles[0]
          #    else:
          #        SRC_PROD_NAME = titles
          #plog(f"[*][ {SRC_PROD_NAME} ]")
          #DST_COLLECTION = translate_prod2col(
          #    [SRC_PROD_NAME], PLATFORM, DST_COL_TEST_PREFIX
          #)  # 20231109

          #
          # DEBUG ID AND NAMES
          #
          plog("SOURCE PRODUCT ID: " + SRC_PROD_ID)
          plog("PLATFORM: " + PLATFORM)
          # Translate source product ID to target collection ID
          # TBD REVIEW HERE CHECK IF COLLECTION EXISTS IN TARGET
          # Run the stac tools [ TBD REVIEW TEST ONLY FOR S2A 20231018 ]
          plog("[>] Run the stac tools [...]")
          TITLE = titles[0]
          # SRC_DIR="./tmp"
          plog("STAC_BIN: " + STAC_BIN)
          plog("PLATFORM: " + PLATFORM)
          plog("TITLE: " + TITLE)
          # PATCH 20231106
          # if PLATFORM == "S5":
          #  plog("Not Patching TITLE: "+TITLE)
          #  if "." in TITLE:
          #    TITLE=TITLE.split(".")[0]+".SAFE"
          #  plog("Not Patching TITLE: "+TITLE)
          plog(f"[*][ TITLE {TITLE}")

        plog("DST_COLLECTION: " + DST_COLLECTION)
        # RUN THE STAC TOOLS
        SRC_DIR = "./"  # this supposes the os.chdir
        plog("SRC_DIR: " + SRC_DIR)
        os.chdir(FDIR_OUT)
        # 20231108 PATCH SAFE

        # 20231114
        # fwrite("metadata.xml",pro_meta,FDIR=TITLE) # 20231116
        # fwrite("metadata.xml",pro_meta,FDIR=TITLE+os.sep+TITLE) # 20231116

        # else:
        #  TITLE=TITLE # +".SAFE"
        # if TITLE.split(".")[1] != "SAFE":
        #  TITLE=TITLE.split(".")[0]+".SAFE"
        #plog(f"[*][ New Title {TITLE}")
        # if PLATFORM=="S5":
        #  STITLE = TITLE.split(".")[0]+".SAFE"
        # run_stac_tools(STAC_BIN, PLATFORM, STITLE, SRC_DIR)
        #if PLATFORM=="S5":
        #run_stac_tools(STAC_BIN, PLATFORM, SRC_PROD_NAME, SRC_DIR)  # 20231116
        #else:
        cmdres,sres=run_stac_tools(STAC_BIN, PLATFORM, SRC_PROD_ID, SRC_DIR)  # 20231116
        # 20231128
        plog(f"[*] EVENT Stac Tools Result {str(sres)})")
        if sres!=0:
          osexit(sres)

        #
        # Get the latest json [ TBD REVIEW TEST ONLY FOR S2A 20231018 ]
        #
        fname = get_json_ls("./", PLATFORM)  # TBD REVIEW
        fname_out = "resto-test_upload.json"  # TBD REVIEW
        plog("fname:" + fname)
        plog("fname out: " + fname_out)

        #
        # Patch the JSON
        #
        ujh_fname_out, ujh_upload_json = update_json_hrefs(
            DST_URL,
            SRC_PROD_ID,
            fname,
            fname_out,
        )

        #
        # VERIFY UPLOAD INFO
        #

        plog("[i] UPLOAD READY")
        plog("[+] RESULT: ")
        # dbg_fname_upload=fname_out
        dbg_src_url = config["source"]["url"]
        dbg_src_user = config["source"]["username"]
        dbg_src_prod_id = SRC_PROD_ID
        dbg_src_prod_name = SRC_PROD_NAME
        dbg_dst_url = config["target"]["url"]
        dbg_dst_user = config["target"]["username"]
        dbg_dst_collection = DST_COLLECTION
        dbg_dst_platform = PLATFORM

        plog("[ ] SRC URL: " + dbg_src_url)
        plog("[ ] SRC USR: " + dbg_src_user)
        plog("[ ] SRC PID: " + dbg_src_prod_id)
        plog("[ ] SRC NAM: " + dbg_src_prod_name)
        plog("[ ] DST URL: " + dbg_dst_url)
        plog("[ ] DST USR: " + dbg_dst_user)
        #plog("[ ] DST COL: " + dbg_dst_collection)
        plog("[ ] DST PFR: " + dbg_dst_platform)

        #
        # WRITE OPERATION [ UPLOAD TO RESTO ]
        #
        # upload_res=upload_collection(config,fname_out,DST_COLLECTION+"",PLATFORM)
        upload_res = upload_collection(config, fname_out, DST_COLLECTION, PLATFORM)

        # DEBUG
        plog(f"[R] Upload Result: {upload_res}")

        # UPLOAD RETURN VALUES
        # print(upload_res)
        if "ErrorMessage" in upload_res:
            plog(upload_res)
            plog(upload_res["ErrorMessage"])
            if "ErrorCode" in upload_res:
                plog("[!] Upload Error Code: #" + str(upload_res["ErrorCode"]))
                # P_EXIT_FAILURE=int(upload_res["ErrorCode"]) # NOTE 20231017 from man
                osexit(P_EXIT_FAILURE)
        if "status" in upload_res:
            if upload_res["status"] == "success":
                plog("[*] Upload status")
                plog("[*] uploaded status: " + str(upload_res["status"]))
                plog("[*] uploaded inserted: " + str(upload_res["inserted"]))
                plog("[*] uploaded inError: " + str(upload_res["inError"]))
                plog(
                    "[*] uploaded featureId: " + upload_res["features"][0]["featureId"]
                )
                plog(
                    "[*] uploaded productIdentified: "
                    + upload_res["features"][0]["productIdentifier"]
                )
                plog("[*] uploaded erorrs: " + str(upload_res["errors"]))
                plog("[+] UPLOAD O.K.")
                with open(
                    "upload_" + upload_res["features"][0]["productIdentifier"] + ".log",
                    "w",
                ) as f:
                  f.write(json.dumps(upload_res))

        # basicauth=None
        if "status" in upload_res:
            # resto_url="resto-test.c-scale.zcu.cz"
            resto_url = "https://" + config["target"]["url"]
            sub_url = (
                "/collections/"
                + DST_COLLECTION
                + "/"
                + "items/"
                + upload_res["features"][0]["featureId"]
            )
            headers = {"Content-Type": "application/json; charset=utf-8"}
            ruser = config["target"]["username"].strip()
            rpass = config["target"]["password"].strip()
            # print(ruser+" "+rpass) # DEBUG
            basicauth = HTTPBasicAuth(ruser, rpass)
            plog(resto_url + sub_url)
            resp = requests.get(resto_url + sub_url, headers=headers, auth=basicauth)
            # print(resp.text) # DEBUG
            upload_verify_res = json.loads(resp.text)
            if "id" in upload_verify_res:
                if upload_verify_res["id"] == upload_res["features"][0]["featureId"]:
                    plog("[+] UPLOAD VERIFY O.K.")
                    # fwrite(SRC_PROD_NAME + "_app_db.json", json.dumps(upload_res))
                    fwrite(SRC_PROD_NAME + ".json", json.dumps(upload_res))
        else:
            plog("[*] Not verifyng upload.")

        plog("[+] PROGRAM COMPLETED. Exiting...")

        # RETURN CONTROL TO SHELL
        osexit(P_EXIT_SUCESS)  # 20231016
    except Exception as e:
        if e is None:
            e = str("Undef")
            exc_handl(e, "[ ERR RS-1000 ][!][ FAILURE IN MAIN. ]")
        # REMOVE THE LOCK FILE
        exc_handl(e, "[ ERR RS-0000 ][!][ FAILURE IN MAIN. ]")
        # rmlock()
        # RETURN CONTROL TO SHELL
        osexit(P_EXIT_FAILURE)  # 20231016


main()
