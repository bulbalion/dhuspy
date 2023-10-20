#!/usr/bin/env python3
# coding: utf-8

PROGRAM_HEADER="""

VERSION: 0.0.1c

Last Update: 20231020
Last Change: test result edits

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

Description:

DHR1 TO RESTO REWRITTEN: register-stac.sh from DHusTools

"""


# INPUT: dhr1 xml files processed by stac tools uploaded to resto catalog
DEBUG=1 # debug leve 3 brings tracebacks, 2 additional messages
# PLOG MSG TYPES
MDEBUG=2
MINFO=1
MWARNING=0
# MAX JSON MB PARSE
MAX_JSON_PARSE=16 # 16 MB
# swap download filename
DOWNLOAD_SWAP_FNAME="tmp_register_stac.swap"
# PROGRAM EXITS VALUES
P_EXIT_SUCESS=0
P_EXIT_FAILURE=1

# CHANGE HERE DOWNLOAD DATA DIRECTORY
FDIR_OUT="/home/user/app/register-stac/tmp/"

# IMPORTS
import bs4 as bs
import configparser
import datetime
import getopt
import inspect
import json
import os
from os import listdir, sep, path
from pathlib import Path
import re
import requests
from requests.auth import HTTPBasicAuth
import subprocess
import sys
import traceback


# REQ 20230801002 Obtains metadata for the given product from DHuS storage | 003
# read node.xml
def fread(file):
  f=open(FDIR_OUT+file,'r')
  txt=f.read()
  f.close()
  return(txt)


# REQ 20230801002 Obtains metadata for the given product from DHuS storage | 002
# save node.xml
def fwrite(file,txt):
  f=open(FDIR_OUT+file,'w')
  f.write(txt)
  f.close()
  return(0)

# inspect
def __FILE__() -> str:
  # ptyhon has a native __file__
  return(inspect.currentframe().f_back.f_code.co_filename)

def __LINE__() -> int:
  # python has no native __line__, the closest thing I could find was: sys._getframe().f_lineno
  return(inspect.currentframe().f_back.f_lineno)

# using datetime module
def plog(message,message_priority=1):
  if message_priority > 0 or DEBUG == 1:
    cdt = datetime.datetime.now()
    print("["+str(cdt)+"]["+str(message_priority)+"]["+inspect.stack()[1].function+"]["+str(inspect.stack()[1].lineno)+"]: "+str(message))


# REQ 20230801003 endpoint specified in configuration - read
# READ CONFIG
def read_ini():
  config = configparser.ConfigParser()
  config.sections()
  config.read('dhus.ini')
  if "source" not in config and "target" not in config:
    if config["source"]["url"] not in config and config["target"]["url"] not in config:
      if config["source"]["url"] not in config and config["target"]["url"] not in config:
        if config["source"]["username"] not in config and config["target"]["username"] not in config:
          if config["source"]["password"] not in config and config["target"]["password"] not in config:
            plog("[!] Config file read problem")
            exit(P_EXIT_FAILURE)
  plog(str(config.sections()))
  plog('CFG SOURCE URL: '+config['source']['url'])
  plog('CFG TARGET URL: '+config['target']['url'])
  #print(config['source']['username'])
  #print(config['source']['password'])
  return(config)

# TEST
#config=read_ini()


# EXCEPTION HANDLER
def exc_handl(e,msg,warning=True):
  if DEBUG>2:
    traceback.print_exc(file=sys.stdout)
  if DEBUG>1:
    plog(msg,2) # CHANGE HERE
  if warning==True:
    plog("[!] Exception message: "+str(e))    


# REQ 20230801003 endpoint specified in configuration - write
# WRITE CONFIG

def create_ini():
  config = configparser.ConfigParser()
  config['general'] = {
    'ServerAliveInterval': '45',
    'Compression': 'yes'
  }
  # SOURCE SERVICE
  config['source'] = {}
  config['source']['url'] = 'dhr1.cesnet.cz'
  #config['source']['username'] = '' # WRITE ONCE READ MANY TIMES
  #config['source']['password'] = '' # WRITE ONCE READ MANY TIMES
  # TARGET SERVICE
  config['target'] = {}
  config['target']['url'] = 'resto-test.c-scale.zcu.cz'
  #config['target']['username'] = '' # WRITE ONCE READ MANY TIMES
  #config['target']['password'] = '' # WRITE ONCE READ MANY TIMES
  with open('dhus.ini', 'w') as configfile:
    config.write(configfile)
   
#create_ini()
# ensure dhu.ini is read only for the owner and set permissions after creation
# $ chmod 400 dhus.ini


# REQ 20230801001 Obtains a product ID from command line attribute - function

def proc_cmd_opts():
  # global ID
  ID=None
  # https://docs.python.org/3/library/getopt.html
  try:
    opts, args = getopt.getopt(sys.argv[1:],[])
    #plog("optlist: "+str(opts))
    #plog("args: "+str(args))
    #plog(len(args))
    if len(args)>0:
      plog(args[0])
      ID=args[0]
      plog("[*] INPUT ID: "+ID)
      return(ID)
    else:
      plog("[*] NO ID ARGUMENT SPECIFIED")
      exit(P_EXIT_FAILURE)
  except getopt.GetoptError as e:
    plog(e + " " + " NO ID ARGUMENT SPECIFIED")
    exit(P_EXIT_FAILURE)

# BASIC INPUT ID VERIFICATION
def check_source_id(src_id):

  ID=str(src_id)
  FIXED_ID_LEN=36
  CTRL_001=0
  CTRL_002=0

  if ID:
    if len(ID) == FIXED_ID_LEN:
      plog("[ CTRL ] ID LENGTH %d ... O.K.]" % FIXED_ID_LEN)
      CTRL_001=1
    try:
      if (ID.split("-")==4):
        CTRL_002=1
    except Exception as e:
      plog(e)
      plog("[!] CTRL_002 Does not comply to the common format.")
      exit(P_EXIT_FAILURE)
  if not CTRL_001 == 1 and not CTRL_002 == 1:
    plog("[!] Both CTRL_001 or CTRL_002 Controls did not pass. ID seems to be malformed.")
    exit(P_EXIT_FAILURE)

# TEST
# proc_cmd_opts()

def get_api_large_file(url,basicauth,is_stream):
  local_filename = DOWNLOAD_SWAP_FNAME
  # NOTE the stream=True parameter below
  with requests.get(url=url,auth=basicauth,stream=is_stream) as r:
    r.raise_for_status()
    with open(local_filename, 'wb') as f:
      for chunk in r.iter_content(chunk_size=8192): 
        # If you have chunk encoded response uncomment if
        # and set chunk_size parameter to None.
        #if chunk: 
        f.write(chunk)
  return(local_filename)


# def 20231005 revert from rtc try to download data
def get_api(hostname,sub_url,user=None,password=None,params=dict(),post=False,is_stream=False):
  #server=hostname
  # VARIABLES
  url='https://'+hostname+sub_url
  #params = dict()
  data=None
  resp=None
  # params = dict(
  #   # key = 'str_value'
  # )
  #
  if DEBUG: plog("URL: "+url)
  #

  try:
    if user:
      from requests.auth import HTTPBasicAuth
      basicauth=HTTPBasicAuth(user, password)
      if post:
        resp = requests.post(url,"https://"+server)
      resp = requests.get(url=url,auth=basicauth,params=params)
    else:
      resp = requests.get(url=url,params=params)
  except Exception as e:
    # ADV DEBUG: plog(resp)
    exc_handl(e,"[!] Cannot download the result page")
  # PARSE THE JSON 
  try:
    if resp:
      data = resp.json() # Check the JSON Response Content documentation below
  except Exception as e:
    #if DEBUG: print(resp.text)
    exc_handl(e,"[!] Cannot parse the json returning the resp.text",warning=False)
    return(resp.text)
  #try:
  #  print(data["title"])
  #except Exception as e:
  #  exc_handl(e,"[!] Cannot get element from the parsed json")
  return(data)

#
# TesT URL ROUTINES
#
def get_collection_metadata():

  server = config["target"]["url"]
  sub_url = "/collections"
  res=get_api(server,sub_url)

  # COLLECTIONS METADATA DEBUG
  plog("[-] VERSION     : "+res["stac_version"])
  plog("[-] ID          : "+res["id"])
  plog("[-] TYPE        : "+res["type"])
  plog("[-] TITLE       : "+res["title"])
  plog("[-] DESCRIPTION : "+res["description"])
  plog("[-] KEYWORDS    : "+str(res["keywords"]))
  
  # iterate collections
  for i in res["collections"]:
    # ADV DEBUG plog(i)
    plog("[-] ID: "+str(i["id"])+" [-] TITLE: "+str(i["title"]))
  return(res)

# source info api print
# res=get_collection_metadata()

# REQ 20230801002 Obtains metadata for the given product from DHuS storage | 001

# Get source metadata
def get_source_metadata(config,P_ID):
  server= config['source']['url']
  sub_url = "/odata/v1/Products('"+P_ID+"')/Nodes"
  res=get_api(server,sub_url,user=config['source']['username'],password=config['source']['password']) # CONF
  #print(res)
  # fwrite("node.xml",res) # HERE
  # ADV DEBUG
  # plog(res.split('\n')[:10])
  return(res)

# TEST 
# get_source_metadata(config,P_ID)

#
# REQ 20230801002 Obtains metadata for the given product from DHuS storage | 004
# GET SOURCE DHR PRODUCTS
# 
# TITLE='xmlstarlet sel -d -T -t -v "//_:entry/_:title" node.xml'
# PREFIX='xmlstarlet sel -d -T -t -v "//_:entry/_:id" node.xml'
#
def update_source_metadata_nodexml(fname):
  xml=fread(fname)
  plog(xml)
  src_xml = bs.BeautifulSoup(xml,features="xml")
  titles=[]
  ids=[]
  #for val in src_xml.find_all('entry')[:3]: # LIMITED
  for val in src_xml.find_all('entry'): # LIMITED
    title=str(val.find('title').get_text())
    titles.append(title)
    plog(title)
    id=str(val.find('id').get_text())
    ids.append(id)
    #plog(id)    
    break
  #plog("TITLE: "+titles[0])
  #ID=titles[0]
  #PLATFORM=ID[0:2]
  #plog("PLATFORM: "+PLATFORM)
  #PREFIX=id
  #plog("PREFIX: "+PREFIX)
  #PRODUCTURL="/".join(id.split("/")[:-1])
  #plog("PRODUCTURL: "+PRODUCTURL)
  #plog("Platform from title:"
  return(titles)

# update node.xml TEST
#CID,PLATFORM,titles=update_source_metadata_nodexml("node.xml")
#plog("CID: "+CID + " PLATFORM: " + PLATFORM)


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
def get_source_metadata_manifest_safe(config,P_ID,TITLE):
  server=config["source"]["url"] # REVIEW TBD HERE
  #api_protocol="https://"
  #server= "dhr1.cesnet.cz"
  sub_url = "/odata/v1/Products('"+P_ID+"')/Nodes('"+TITLE+"')"+"/"+"Nodes('manifest.safe')/$value"
  res=get_api(server,sub_url,user=config['source']['username'],password=config['source']['password']) # CONF
  # ADV DEBUG plog(res.split('\n')[:10])
  if res:
    # CREATE DIR IF IT DOES NOT EXISTS
    try:
      os.mkdir(FDIR_OUT+TITLE)
    except:
      plog("[*] PRODUCT DIR ALREADY EXISTS "+TITLE)
    fwrite(TITLE+os.sep+"manifest.safe",res)
    plog("[o] manifest.safe file save : "+TITLE+os.sep+"manifest.safe")

# TEST
# get_source_metadata_manifest_safe(config,P_ID,TITLE)


# REQ 20230801004 Downloads additional metadata files required by stac-tools
# for the given product type. The actual list of files do download depends on 
# product type.
# -> IN VARIABLE: manifest.safe, SAVES: manifest.safe
# TBD: xfdumanifest.xml, etc. different handling for different platforms. [ "S1", "S2", "S3", "S3p"
# FOR S1 and S2 get MTD_MSIL2
# /MTD_MSIL2A.xml|MTD_MSIL1C.xml|/MTD_TL.xml|annotation/s1a.*xml" 
# sed 's/.*href="//' | sed 's/".*//' |
# get_api(hostname,sub_url,params=dict(),user=None,password=None):

def get_product_metadata(config,P_ID):
  #server= "dhr1.cesnet.cz"
  #api_protocol="https://"
  server=config["source"]["url"] # REVIEW TBD HERE
  sub_url = "/odata/v1/Products('"+P_ID+"')"
  res=get_api(server,sub_url,user=config['source']['username'],password=config['source']['password']) # CONF
  # ADV DEBUG
  # plog(res)
  #if res:
  #  fwrite("manifest.safe",res)
  return(res)

# TEST
# get_source_metadata_manifest_safe()


# REQ 20230801004 Downloads additional metadata files required by stac-tools 
# for the given product type. The actual list of files do download depends on 
# product type.
# READS AND PARSES manifest.safe, extracts metadata filenames and paths
# print(ID)
def get_source_metadata_all(ID,TITLE):
  #os.makedirs(FDIR_OUT+TITLE,exist_ok=True) # UNSAFE
  mnfst=fread(TITLE+os.sep+"manifest.safe")
  #fwrite(ID+os.sep+"manifest.safe",mnfst)
  src_mnfst = bs.BeautifulSoup(mnfst,features="xml")
  #for val in src_mnfst.find_all('entry')[:3]: # LIMITED
  #  pass
  file_locs=[]
  for val in src_mnfst.find_all('fileLocation'): # NOT LIMITED
    # 20231004 MP added tiff filter
    #if ('.tiff' not in val.get('href') and '.jp2' not in val.get('href') and '.gml' not in val.get('href')): # GET ONLY METADATA NODES NAMES
    #if ('.tiff' not in val.get('href') and '.jp2' not in val.get('href') and '.gml' not in val.get('href')): # GET ONLY METADATA NODES NAMES
    # GET ONLY METADATA NODES NAMES
    if ('.tiff' not in val.get('href')):
      if ('.jp2' not in val.get('href')):
        if ('.gml' not in val.get('href')):
          HREF=val.get('href')
          file_locs.append(HREF)
          if HREF[:2]=="./":
            tmp_href=HREF[2:]
    #FNAME=FDIR_OUT+TITLE+os.sep+(os.sep.join(tmp_href.split(os.sep)))
    #plog("href: "+str(HREF))
    #plog("fname: "+str(FNAME))
  #for idx, loc in enumerate(file_locs):
  #  plog("[ "+str(idx)+" ][ "+loc+" ]")
  src_fnames=[]
  src_fpaths=[]
  for fname in file_locs:
    #print(fname.split('/')[-1])
    src_fnames.append(fname.split('/')[-1])
    src_fpaths.append('/'.join(fname.replace('./','').split('/')[:-1]))
    #src_fpaths.append('/'.join(fname.split('/')[:-1]))
  # ADV DEBUG plog(src_fnames)
  # ADV DEBUG plog(src_paths)
  #for idx, loc in enumerate(src_fnames):
  #  plog("[ "+str(idx)+" ][ "+loc+" ] [ "+src_fpaths[idx]+" ]")
  return(src_fnames,src_fpaths)

# TEST
#get_source_metadata_all(ID)

def get_metadata_file(TITLE,config,urls,src_fpaths,src_fnames):
  src_server=config['source']['url']
  for x in range(len(urls)):
    tfname=TITLE+os.sep+src_fpaths[x]+os.sep+src_fnames[x]
    tdir=FDIR_OUT+os.sep+TITLE+os.sep+src_fpaths[x]
    newdir = Path(tdir)
    newdir.mkdir(parents=True,exist_ok=True) # 20231019
    print("[+] mkdir: "+tdir+" ... [ O.K. ]")
    plog("urls ["+str(x)+"]: "+urls[x]+" -> "+tdir)
    # 20231020
    res=get_api(src_server,urls[x],user=config['source']['username'],password=config['source']['password'],is_stream=False)
    if res:
      fwrite(tfname,res)
      print("[v] Download: "+urls[x]+" ... [ O.K. ]")
    else:
      print("[!] failed to download: "+urls[x]+" ... [ X ]")

# PATIENCE (takes cca 10 secs., opt. candidate)
def metadata_json_patch(config,server,src_fnames,src_paths,PROD_ID,NODE_NAME):
  urls=[]
  for x in range(len(src_fnames)):
    subprod=""
    if len(src_paths[x])>1:
      elem=src_paths[x]+"/"+src_fnames[x]
    else:
      elem=src_fnames[x]
    if elem[-1]=='/': elem[:-1] # UGLY UGLY UGLY
    #subprod+="/Nodes('"+elem+"')" 
    subprod="/Nodes('"+elem.replace("/","')/Nodes('")+"')"
    

    plog("MJPE: "+elem)
    #url = "/odata/v1/Products('"+PROD_ID+"')/Nodes('"+NODE_NAME+"')"+subprod+"" # 20231020
    url = "/odata/v1/Products('"+PROD_ID+"')/Nodes('"+NODE_NAME+"')"+subprod+"/$value"
    # plog(str(elem)+" -> "+urls[x]+" -> "+src_fnames[x])
    # plog(url)
    urls.append(url)
  #
  #plog(src_fnames)
  #plog(urls)
  #
  return(urls,src_paths,src_fnames)


# TEST
# src_server=server
# metadata_json_patch(config,src_server,src_fnames,src_paths),P_ID,SID

# REQ 20230801004 Determines which collection the product belongs to
# MAPS SOURCE PRODUCT NAMES TO TARGET NAMES COLLECTIONS

def translate_prod2col(titles,PLATFORM,test_col_prefix="mp-"):
  # TEST ONLY
  #test_col_prefix="mp-"

  rearr= [
    ['^S1[A-DP]_.._GRD[HM]_.*','sentinel-1-grd'],
    ['^S1[A-DP]_.._SLC__.*','sentinel-1-slc'],
    ['^S1[A-DP]_.._RAW__.*','sentinel-1-raw'],
    ['^S1[A-DP]_.._OCN__.*','sentinel-1-ocn'],
    ['^S2[A-DP]_MSIL1B_.*','sentinel-2-l1b'],
    ['^S2[A-DP]_MSIL1C_.*','sentinel-2-l1c'],
    ['^S2[A-DP]_MSIL2A_.*','sentinel-2-l2a'],
    ['^S3[A-DP]_OL_1_.*','sentinel-3-olci-l1b'],
    ['^S3[A-DP]_OL_2_.*','sentinel-3-olci-l2'],
    ['^S3[A-DP]_SL_1_.*','sentinel-3-slstr-l1b'],
    ['^S3[A-DP]_SL_2_.*','sentinel-3-slstr-l2'],
    ['^S3[A-DP]_SR_1_.*','sentinel-3-stm-l1'],
    ['^S3[A-DP]_SR_2_.*','sentinel-3-stm-l2'],
    ['^S3[A-DP]_SY_1_.*','sentinel-3-syn-l1'],
    ['^S3[A-DP]_SY_2_.*','sentinel-3-syn-l2'],
    ['^S5[A-DP]_OFFL_L1_.*','sentinel-5p-l1'],
    ['^S5[A-DP]_NRTI_L1_.*','sentinel-5p-l1'],
    ['^S5[A-DP]_OFFL_L2_.*','sentinel-5p-l2'],
    ['^S5[A-DP]_NRTI_L2_.*','sentinel-5p-l2']
  ]
  
  res_title=None
  
  for x in titles:
    #print(re.sub('^S2[A-DP]_MSIL2A_.*','sentinel-2-l2a',x))
    for rule in rearr:
        res_title=re.sub(rule[0],rule[1],x)
        if res_title:
            if x != res_title:
              plog(str(x)+' -> '+test_col_prefix+res_title)
              break
  return(test_col_prefix+res_title)
  # s/^S2[A-DP]_MSIL2A_.*/sentinel-2-l2a/


# REQ 20230801005 Runs stac-tools to generate a STAC Item description for the product | 001
def cmd_stac(params):
  result = subprocess.run(params,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  #plog("RES: "+str(result))
  return(result)


# REQ 20230801005 Runs stac-tools to generate a STAC Item description for the product | 002
# ERR: FileNotFoundError: [Errno 2] No such file or directory:
# TBD: COMPARE register-stac.sh downloaded metadata files to this script downloaded metadata
# ERR: stac-tools returns: SyntaxError: prefix 'n1' not found in prefix map
# WARNINGS: FixWindingWarning: The exterior ring of this shape is wound clockwise. 
# '/mnt/sdb1/DHusTools/tmp/mp-sentinel-2-l2a/metadata.xml'
def run_stac_tools(STAC_BIN,platform,title,SRC_DIR="."):
  #plog(str(title))
  #TBD: Explore windingw no fix
  params=[]
  # 20230921
  if platform == "S1":
    params=[ STAC_BIN, 'sentinel1', 'grd','create-item', FDIR_OUT+title, FDIR_OUT+SRC_DIR ]
  elif platform == "S2":
    params=[ STAC_BIN, 'sentinel2', 'create-item', title, SRC_DIR]
  elif platform == "S3":
    params=[ STAC_BIN, 'sentinel3', 'create-item', FDIR_OUT+title,FDIR_OUT+SRC_DIR]
  elif platform == "S5":
    params=[ STAC_BIN, 'sentinel5p', 'create-item', FDIR_OUT+title,FDIR_OUT+SRC_DIR]
  plog("CALL STAC: "+str(' '.join(params)))
  #cmdres=cmd_stac(['ls','-l'])
  cmdres=cmd_stac(params)
  if cmdres.returncode==0:
    for stdres in cmdres.stdout:
      plog(stdres)
  else:
    for stdres in cmdres.stderr.decode().split("\n"):
      plog(stdres)
  return(cmdres.stdout)
  ################################

# TBD: different sentinels - VERIFY 20230921
# TEST
# run_stac_tools(PLATFORM,STAC_BIN,TITLE)

#if [ "$PLATFORM" == "S2" ]; then
#	~/.local/bin/stac sentinel2 create-item "${TITLE}" ./
#elif [ "$PLATFORM" == "S1" ]; then
#	~/.local/bin/stac sentinel1 grd create-item "${TITLE}" ./
#elif [ "$PLATFORM" == "S3" ]; then
#	~/.local/bin/stac sentinel3 create-item "${TITLE}" ./
#elif [ "$PLATFORM" == "S5" ]; then
#	~/.local/bin/stac sentinel5p create-item "${TITLE}" ./
#fi


# REQ 20230801006 Modifies asset URLs to match actual URLs in DHuS.

# TBD: GET THE NEWEST json IN THE GIVEN DIR
def ls(path):
  f = []
  try:
   for fname in listdir(path):
      f.append(path+sep+fname)
   f.sort()
  except Exception as e:
    f = [ "[ "+str(e)+"]" ]
  return(f)


# REQ 20230801006 Modifies asset URLs to match actual URLs in DHuS.
# find first json file [ almost random TBD: algoritmize precisiation ]
# and use it in next processing
def get_json_ls(idir=".",PLATFORM="S2A"):
  dls=ls(idir)
  fname=None
  for fname in dls:
    if "json" in fname and PLATFORM in fname: # 20230919 HERE S2A to S2
      return(fname)

# TEST
#fname=get_json_ls()
#print(fname)


# REQ 20230801006 Modifies asset URLs to match actual URLs in DHuS.
def update_json_hrefs(fname,fname_out="resto-test_upload.json",AID=None):
  debug_test_href=""
  fcjson=fread(fname)
  djson=json.loads(fcjson)
  #print(djson)
  orig_json=djson
  djson=orig_json
  upload_json=orig_json
  #json_id=''.join(AID.split('.')[:-1])
  json_id="dhr1"+AID # TBD REVIEW HERE
  upload_json["id"]=json_id
  for key in djson.keys():
    if key == "assets": # ASSETS DRILL DOWN
      try:
        #print(str(key))
        for x in djson[key]: # FOR EACH ASSETS
          #print(x)
          #for l in x: # SEEK FOR HREF
          href=djson[key][x].get("href") # GET HREF
          if href: # IF HREF
            #print(href)
            url="" 
            new_href="Nodes('"+href.replace("/","')/Nodes('")+"')" 
            if href[-1]=='/': href=href[:-1] # TBD: PREFFITY [ UGLY UGLY UGLY ]
            #url=HOST+"odata/v1/Products('"+P_ID+"')/"+new_href+"/$value"
            #url=HOST+"odata/v1/Products('"+P_ID+"')/"+new_href+"/$value"
            url=HOST+"odata/v1/Products('"+P_ID+"')/"+new_href # 20231019
            ##djson[key][x]["href"].value=url
            #djson[key][x]["href"]=url
            upload_json[key][x]["href"]=url
            if debug_test_href == "":
              #print(upload_json[key][x]["href"])
              debug_test_href=upload_json[key][x]["href"]
      except:
        pass
  plog("res: "+str(upload_json)[:100]+" ...")
  plog("res href: "+debug_test_href)
  upload_json=djson
  djson=orig_json
  fwrite(fname_out,json.dumps(upload_json))
  #print(upload_json)

# TEST
#fname_out="resto-test_upload.json"
#update_json_hrefs(fname,fname_out)
#fwrite("resto-test_upload.json",json.dumps(upload_json))


# TBD: BASIC JSON CHECKS


# REQ 20230801008 ploads the resulting STAC Item into the catalogue (endpoint in configuration)
# curl -n -o output.json -X POST "${STACHOST}/collections/${COLLECTION[${PLATFORM}]}/items"
# -H 'Content-Type: application/json' -H     'Accept: application/json'
# --upload-file "new_${file}"
def view_col_items(DST_COLLECCTION):
  #basicauth=None
  #resto_url="resto-test.c-scale.zcu.cz"
  server_protocol="https://"
  server=config["target"]["url"] # REVIEW TBD HERE
  #sub_url="/collections"+DST_COLLECTION+"["+PLATFORM+"]"+"/"+"items"
  sub_url="/collections" # /"+DST_COLLECTION+"["+PLATFORM+"]"
  sub_url="/collections/"+DST_COLLECTION+"/"+"items" # ?
  #sub_url="/collections/"+DST_COLLECTION+"/"+"items"
  #ruser=fread("resto_user.txt").strip()
  #rpass=fread("resto_pass.txt").strip()
  ruser=config["target"]["username"].strip() # REVIEW TBD HERE
  rpass=config["target"]["password"].strip() # REVIEW TBD HERE
  # print(ruser+" "+rpass) # DEBUG
  basicauth=HTTPBasicAuth(ruser, rpass)
  resp = requests.get(server_protocol+resto_url+sub_url,auth=basicauth)
  #print(resp.text) # DEBUG
  data=json.loads(resp.text)
  #print(data["title"])
  return(data)


# TBD: TRANSLATE COLLECTIO NAME FROM ID
# TBD: VERIFY THE UPLOADED JSON
# curl -n -o output.json -X POST "${STACHOST}/collections/${COLLECTION[${PLATFORM}]}/items"
# -H 'Content-Type: application/json' -H     'Accept: application/json'
#--upload-file "new_${file}"

def upload_collection(config,fname_out,STAC_COL,PLATFORM):
  #basicauth=None
  resto_url="resto-test.c-scale.zcu.cz"
  resto_url=config["target"]["url"] # REVIEW TBD HERE
  #sub_url="/collections/"+STAC_COL+"["+PLATFORM+"]"+"/"+"items"
  sub_url="/collections/"+STAC_COL+"/"+"items"
  headers = {"Content-Type": "application/json; charset=utf-8"}
  #ruser=config['target']['username'].strip()
  #rpass=config['target']['password'].strip()
  ruser=config["target"]["username"].strip() # REVIEW TBD HERE
  rpass=config["target"]["password"].strip() # REVIEW TBD HERE
  # print(ruser+" "+rpass) # DEBUG
  basicauth=HTTPBasicAuth(ruser, rpass)
  plog(resto_url+sub_url)
  #noway
  #files = {'file': open('tmp.json','rb')}
  #open(path_file,'r').read()
  json_data=fread(fname_out)
  #files={'file': fobj})    
  resp = requests.post("https://"+resto_url+sub_url,headers=headers,data=json_data,auth=basicauth)
  #print(resp.text) # DEBUG
  data=json.loads(resp.text)
  #print(data["title"])
  #print(data)
  return(data)
    
# TBD: test reupload
# upload_collection()


# curl -n -o output.json -X POST "${STACHOST}/collections/${COLLECTION[${PLATFORM}]}/items"
# -H 'Content-Type: application/json' -H     'Accept: application/json'
#--upload-file "new_${file}"

def test_resto_api(config):
  #basicauth=None
  data=None
  server_protocol="https://"
  #resto_url="resto-test.c-scale.zcu.cz"
  resto_url=config["target"]["url"] # REVIEW TBD HERE
  sub_url=""
  headers = {"Content-Type": "application/json; charset=utf-8"}
  #ruser=fread("resto_user.txt").strip()
  #rpass=fread("resto_pass.txt").strip()
  #ruser=config['target']['username'].strip()
  #rpass=config['target']['password'].strip() 
  ruser=config["target"]["username"].strip() # REVIEW TBD HERE
  rpass=config["target"]["password"].strip() # REVIEW TBD HERE
  #print(ruser+" "+rpass) # DEBUG
  basicauth=HTTPBasicAuth(ruser, rpass)
  plog(resto_url+sub_url)
  resp = requests.get(server_protocol+resto_url+sub_url,headers=headers,auth=basicauth)
  #print(resp.text) # DEBUG
  #noway
  #files = {'file': open('tmp.json','rb')}
  #open(path_file,'r').read()
  #json_data=fread(fname_out)
  #files={'file': fobj})    
  
  data=json.loads(resp.text)
  #print(data["title"])
  #print(data)
  return(data)
    
# TEST URL ROUTINES

def test_target_url(config):
  #server= "resto-test.c-scale.zcu.cz"
  server = config["target"]["url"]
  sub_url = "/collections"
  res=get_api(server,sub_url)

  # COLLECTIONS METADATA
  plog("[-] VERSION     : "+res["stac_version"])
  plog("[-] ID          : "+res["id"])
  plog("[-] TYPE        : "+res["type"])
  plog("[-] TITLE       : "+res["title"])
  plog("[-] DESCRIPTION : "+res["description"])
  plog("[-] KEYWORDS    : "+str(res["keywords"]))
  
  # iterate collections
  for i in res["collections"]:
    plog("[-] ID: "+str(i["id"])+" [-] TITLE: "+str(i["title"]))
  return(res)

#  GET PRODUCT NAME BY ID
def get_product_name_by_id(pro_meta):
  SID=None
  try:
    pro_meta_arr=bs.BeautifulSoup(pro_meta,features="xml")
    for val in pro_meta_arr.find_all('properties'):
      for v in val.find_all('Name'):
        #plog("PROD NAME: "+v.get_text())
        SID=v.get_text()
    return(SID)
  except Exception as e:
    plog(e)
    plog("[!] Cannot read product from "+HOST)
    exit(P_EXIT_FAILURE)


# source info api print
# res=test_target_url(config)


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
  # VARIABLES
  #
  SRC_PROD_ID=None
  
  # MAIN ERROR RETURN CODE
  P_EXIT_FAILURE=1

  # FILE VARIABLES
  SRC_PROD_ID=proc_cmd_opts()
  check_source_id(SRC_PROD_ID)
  
  # READ THE CONFIGURATION
  config=read_ini()
  #res=test_target_url(config)

  # STAC_BIN='/opt/conda/bin/stac' # egi notebooks
  home_folder = os.getenv('HOME')
  # common pip local install
  #STAC_BIN=home_folder+'/.local/bin/stac' 
  STAC_BIN='/usr/local/bin/stac' 
  
  DST_COL_TEST_PREFIX="mp-"

  SRC_URL=config["source"]["url"] 
  DST_URL=config["target"]["url"]
  #STACHOST=config["target"]["url"]
  #P_ID=ID
  TMP="/tmp"
  SUCCPREFIX="/var/tmp/register-stac-success-"
  ERRPREFIX="/var/tmp/register-stac-error-"
  SALT="dhr1"
  sub_url = "/collections"
  
  plog("[*] SRC_URL: "+SRC_URL)
  plog("[*] DST URL: "+DST_URL)
  
  #
  # Retrieve product metadata from source
  #
  pro_meta=get_product_metadata(config,SRC_PROD_ID)
  
  SRC_PROD_NAME=None

  # ADV DEBUG 
  # plog(pro_meta)
  SRC_PROD_NAME=get_product_name_by_id(pro_meta)
  PLATFORM=SRC_PROD_NAME[:2] 
  TITLE=SRC_PROD_NAME+".SAFE"
  
  #PLATFORM=SID[:2] # TBD REVIEW HERE
  # ? from CID,PLATFORM,titles=update_source_metadata_nodexml("node.xml")

  # DEBUG RUNTIME CHECK 
  plog("SOURCE PRODUCT ID   : " + SRC_PROD_ID)
  plog("SOURCE PRODUCT NAME : " + SRC_PROD_NAME)
  plog("SOURCE USER         : " + config["source"]["username"])
  plog("SOURCE HOST         : " + SRC_URL) 
  plog("TARGET USER         : " + config["target"]["username"])
  plog("TARGET HOST         : " + DST_URL)
  plog("TITLE               : " + TITLE)
  plog("PLATFORM            : " + PLATFORM)

  #
  # TEST SOURCE AND TARGET AVAILABILITY
  #
  
  tra=test_resto_api(config)
  if "stac_version" in tra:
    plog("STAC VERSION: "+tra["stac_version"])
    plog("TARGET SERVER UP")
  

  # TBD REVIEW  
  # res=test_url_routines(config)
  
  #
  # get source manifest.safe
  #
  #
  plog("get_source_metadata_manifest_safe(config,SRC_PROD_ID,TITLE)")
  get_source_metadata_manifest_safe(config,SRC_PROD_ID,TITLE)

  
  # retrieve all source metadata
  metadata=get_source_metadata_all(SRC_PROD_NAME,TITLE)

  # plog(metadata)

  # download node.xml from source
  gsm=get_source_metadata(config,SRC_PROD_ID)
  plog("GSM: "+gsm)
  if (gsm):
    # fnodexml=os.sep+TITLE+os.sep+"node.xml" # n1 bug at the stac runtime
    fnodexml="node.xml"
    fwrite(fnodexml,str(gsm)) # HERE
  else:
    plog("Get Source Metadata Returns No Data.")
  plog(fnodexml)
  plog("GSM:" + str(gsm))
  
  #
  # Update node.xml source metadata
  #
  titles=update_source_metadata_nodexml(fnodexml)

  # ADV DEBUG
  # plog("P_ID "+SRC_PROD_ID)
  # plog("TITLE "+TITLE)
  # plog("titles"+str(titles))
  # plog(SRC_PROD_ID)

  # plog("titles: "+str(titles)+" "+SRC_PROD_ID)
  # plog("SRC_PRODUCT_NAME: "+SRC_PROD_NAME)
  # PLATFORM=P_ID[0:2]

  #
  # GET DESTIONATION COLLECTION ID FROM PRODUCT ID
  #
  DST_COLLECTION=translate_prod2col(titles,PLATFORM)

  plog("DST_COLLECTION: " + DST_COLLECTION)

  
  # PLACEHOLDER FOR FIXED PROC_CMD_OPTS
  
  #
  # Get the manifest.safe
  #
  #get_source_metadata_manifest_safe(config,SRC_PROD_ID,TITLE)

  #
  # GET ALL SOURCE METADATA
  #
  src_fnames,src_paths=get_source_metadata_all(SRC_PROD_NAME,TITLE)


  # ADV DEBUG 
  #plog(src_fnames[:3])
  #plog(src_paths[:3])
  
  #
  # Patch the metadata
  #
  # #src_names,urls = metadata_json_patch(config,server,src_fnames,src_paths,PROD_ID,NODE_NAME,prev_prod="'MTD_MSIL2A.xml'"):
  urls2,src_fpaths2,src_fnames2=metadata_json_patch(config,SRC_URL,src_fnames,src_paths,SRC_PROD_ID,TITLE)
  
  #plog("URLS2: "+str(urls2))
  get_metadata_file(TITLE,config,urls2,src_fpaths2,src_fnames2)
  #
  # DEBUG ID AND NAMES
  #
  plog("SOURCE PRODUCT ID: "+SRC_PROD_ID)
  plog("DST_COLLECTION: "+DST_COLLECTION)
  plog("PLATFORM: "+PLATFORM)
  plog("src_fanems[:3} "+str(src_fnames[:3]))
  plog("src_fpaths[:3] "+str(src_fpaths2[:3]))
  plog("urls2[:3] "+str(urls2[:3]))
  #
  # Translate source product ID to target collection ID
  #
  # TBD REVIEW HERE CHECK IF COLLECTION EXISTS IN TARGET
  #
  DST_COLLECTION=translate_prod2col(titles,DST_COL_TEST_PREFIX)

  #
  # Run the stac tools [ TBD REVIEW TEST ONLY FOR S2A 20231018 ]
  #
  TITLE=titles[0]
  #SRC_DIR="./tmp"
  plog("STAC_BIN: "+STAC_BIN)
  plog("PLATFORM: "+PLATFORM)
  plog("TITLE: "+TITLE)
  plog("DST_COLLECTION: "+DST_COLLECTION)

  # RUN THE STAC TOOLS
  SRC_DIR="." # this supposes the os.chdir
  plog("SRC_DIR: "+SRC_DIR)
  os.chdir(FDIR_OUT)
  run_stac_tools(STAC_BIN,PLATFORM,TITLE,".")
  
  #  
  # Get the latest json [ TBD REVIEW TEST ONLY FOR S2A 20231018 ]
  #
  fname=get_json_ls("./",PLATFORM) # TBD REVIEW
  fname_out="resto-test_upload.json" # TBD REVIEW
  plog("fname:"+fname)
  plog("fname out: "+fname_out)
  
  # 
  # Patch the JSON
  # 
  update_json_hrefs(fname,fname_out,SRC_PROD_ID)
  
  #
  # WRITE OPERATION [ UPLOAD TO RESTO ]
  #
  #upload_res=upload_collection(config,fname_out,DST_COLLECTION+"",PLATFORM)
  upload_res=upload_collection(config,fname_out,DST_COLLECTION,PLATFORM)

  # DEBUG plog(upload_res)
  
  # UPLOAD RETURN VALUES
  #print(upload_res)
  if "ErrorMessage" in upload_res:
      plog(upload_res)
      plog(upload_res["ErrorMessage"])
      if "ErrorCode" in upload_res:
        plog("[!] Upload Error Code: #"+str(upload_res["ErrorCode"]))
        # P_EXIT_FAILURE=int(upload_res["ErrorCode"]) # NOTE 20231017 from man
        exit(P_EXIT_FAILURE)
  if "status" in upload_res:
    if upload_res["status"]=="success":
      plog("[*] uploaded status: "+str(upload_res["status"]))
      plog("[*] uploaded inserted: "+str(upload_res["inserted"]))
      plog("[*] uploaded inError: "+str(upload_res["inError"]))
      plog("[*] uploaded featureId: "+upload_res["features"][0]["featureId"])
      plog("[*] uploaded productIdentified: "+upload_res["features"][0]["productIdentifier"])
      plog("[*] uploaded erorrs: "+str(upload_res["errors"]))
      plog("[+] UPLOAD O.K.")
      f=open("upload_"+upload_res["features"][0]["productIdentifier"]+".log","w")
      f.write(json.dumps(upload_res))
      f.close()
  
  #basicauth=None
  if "status" in upload_res:
    #resto_url="resto-test.c-scale.zcu.cz"
    resto_url = "https://"+config["target"]["url"]
    sub_url="/collections/"+DST_COLLECTIONS+"/"+"items/"+upload_res["features"][0]["featureId"]
    headers = {"Content-Type": "application/json; charset=utf-8"}
    ruser=config['target']['username'].strip()
    rpass=config['target']['password'].strip()
    # print(ruser+" "+rpass) # DEBUG
    basicauth=HTTPBasicAuth(ruser, rpass)
    plog(resto_url+sub_url)
    resp = requests.get(resto_url+sub_url,headers=headers,auth=basicauth)
    #print(resp.text) # DEBUG
    upload_verify_res=json.loads(resp.text)
    if "id" in upload_verify_res:
      if upload_verify_res["id"]==upload_res["features"][0]["featureId"]:
        plog("[+] UPLOAD VERIFY O.K.")
  else:
    plog("[*] Not verifyng upload.")
  
  plog("[+] PROGRAM COMPLETED. Exiting...")
  
  exit(P_EXIT_SUCESS) # 20231016
  
