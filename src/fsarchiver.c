/*
 * fsarchiver: Filesystem Archiver
 *
 * Copyright (C) 2008-2018 Francois Dupoux.  All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License v2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * Homepage: http://www.fsarchiver.org
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <getopt.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#include "fsarchiver.h"
#include "dico.h"
#include "common.h"
#include "oper_restore.h"
#include "oper_save.h"
#include "oper_probe.h"
#include "archinfo.h"
#include "syncthread.h"
#include "comp_lzo.h"
#include "comp_lz4.h"
#include "crypto.h"
#include "options.h"
#include "logfile.h"
#include "error.h"
#include "queue.h"

char *valid_magic[]={FSA_MAGIC_MAIN, FSA_MAGIC_VOLH, FSA_MAGIC_VOLF,
    FSA_MAGIC_FSIN, FSA_MAGIC_FSYB, FSA_MAGIC_DATF, FSA_MAGIC_OBJT,
    FSA_MAGIC_BLKH, FSA_MAGIC_FILF, FSA_MAGIC_DIRS, NULL};

void usage(char *progname, bool examples)
{
    int lzo, lzma, lz4, zstd;

#ifdef OPTION_LZO_SUPPORT
    lzo=true;
#else
    lzo=false;
#endif // OPTION_LZO_SUPPORT
#ifdef OPTION_LZMA_SUPPORT
    lzma=true;
#else
    lzma=false;
#endif // OPTION_LZMA_SUPPORT
#ifdef OPTION_LZ4_SUPPORT
    lz4=true;
#else
    lz4=false;
#endif // OPTION_lZ4_SUPPORT
#ifdef OPTION_ZSTD_SUPPORT
    zstd=true;
#else
    zstd=false;
#endif // OPTION_ZSTD_SUPPORT
    msgprintf(MSG_FORCE, "====> fsarchiver version %s (%s) - http://www.fsarchiver.org <====\n", FSA_VERSION, FSA_RELDATE);
    msgprintf(MSG_FORCE, "Distributed under the GPL v2 license (GNU General Public License v2).\n");
    msgprintf(MSG_FORCE, " * usage: %s [<options>] <command> <archive> [<dev1> [<dev2> [...]]]\n", progname);
    msgprintf(MSG_FORCE, "<commands>\n");
    msgprintf(MSG_FORCE, " * savefs: save filesystems to an archive file (backup a device to a file)\n");
    msgprintf(MSG_FORCE, " * restfs: restore filesystems from an archive (overwrites the existing data)\n");
    msgprintf(MSG_FORCE, " * savedir: save directories to the archive (similar to a compressed tarball)\n");
    msgprintf(MSG_FORCE, " * restdir: restore data from an archive which is not based on a filesystem\n");
    msgprintf(MSG_FORCE, " * archinfo: show information about an existing archive file and its contents\n");
    msgprintf(MSG_FORCE, " * probe [detailed]: show list of filesystems detected on the disks\n");
    msgprintf(MSG_FORCE, "<options>\n");
    msgprintf(MSG_FORCE, " -A: allow to save a filesystem which is mounted in read-write (live backup)\n");
    msgprintf(MSG_FORCE, " -a: allow to save a filesystem when acls and xattrs are not supported\n");
    msgprintf(MSG_FORCE, " -B <mbsize>: set the size of PER THREAD read() request buffer, in MB. MAX is 32\n");
    msgprintf(MSG_FORCE, " -c <password>: encrypt/decrypt data in archive, \"-c -\" for interactive password\n");
    msgprintf(MSG_FORCE, " -d: debug mode (can be used several times to increase the level of details)\n");
    msgprintf(MSG_FORCE, " -e <pattern>: exclude files and directories that match that pattern\n");
    msgprintf(MSG_FORCE, " -h: show help and information about how to use fsarchiver with examples\n");
    msgprintf(MSG_FORCE, " -J <count>: create more than one file read() thread for save commands. useful on RAID\n");
    msgprintf(MSG_FORCE, " -j <count>: create more than one (de)compression thread. useful on multi-core cpu\n");
    msgprintf(MSG_FORCE, " -L <label>: set the label of the archive (comment about the contents)\n");
    msgprintf(MSG_FORCE, " -M <mbsize>: set the MAXIMUM size of in-memory block-caching of file content. NOTE: MINIMUM arg value is 1 (MB), and if value is > real memory, MAXIMUM is reset to 1/2 of real memory. NOTE: This arg excludes a variety of process-specific memory uses / only relates to file content caching.\n");
    msgprintf(MSG_FORCE, " -o: overwrite the archive if it already exists instead of failing\n");
    msgprintf(MSG_FORCE, " -s <mbsize>: split the archive into several files of <mbsize> megabytes each\n");
    msgprintf(MSG_FORCE, " -V: show program version and exit\n");
    msgprintf(MSG_FORCE, " -v: verbose mode (can be used several times to increase the level of details)\n");
    msgprintf(MSG_FORCE, " -x: enable support for experimental features (they are disabled by default)\n");
#ifdef OPTION_ZSTD_SUPPORT
    msgprintf(MSG_FORCE, " -Z <level>: zstd compression level from 1 (very fast) to 22 (very good), 0=No compression\n");
#endif // OPTION_ZSTD_SUPPORT
    msgprintf(MSG_FORCE, " -z <level>: legacy compression level from 0 (very fast) to 9 (very good)\n");
    msgprintf(MSG_FORCE, "<information>\n");
    msgprintf(MSG_FORCE, " * Support included for: lzo=%s, lzma=%s, lz4=%s, zstd=%s\n", (lzo==true)?"yes":"no", (lzma==true)?"yes":"no", (lz4==true)?"yes":"no", (zstd==true)?"yes":"no");
    msgprintf(MSG_FORCE, " * Support for ntfs filesystems is unstable: don't use it for production.\n");

    if (examples==true)
    {
        msgprintf(MSG_FORCE, "<examples>\n");
        msgprintf(MSG_FORCE, " * \e[1msave only one filesystem (/dev/sda1) to an archive:\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver savefs /data/myarchive1.fsa /dev/sda1\n");
        msgprintf(MSG_FORCE, " * \e[1msave two filesystems (/dev/sda1 and /dev/sdb1) to an archive:\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver savefs /data/myarchive2.fsa /dev/sda1 /dev/sdb1\n");
        msgprintf(MSG_FORCE, " * \e[1mrestore the first filesystem from an archive (first = number 0):\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver restfs /data/myarchive2.fsa id=0,dest=/dev/sda1\n");
        msgprintf(MSG_FORCE, " * \e[1mrestore the second filesystem from an archive (second = number 1):\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver restfs /data/myarchive2.fsa id=1,dest=/dev/sdb1\n");
        msgprintf(MSG_FORCE, " * \e[1mrestore two filesystems from an archive (number 0 and 1):\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver restfs /data/arch2.fsa id=0,dest=/dev/sda1 id=1,dest=/dev/sdb1\n");
        msgprintf(MSG_FORCE, " * \e[1mrestore a filesystem from an archive and convert it to reiserfs:\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver restfs /data/myarchive1.fsa id=0,dest=/dev/sda1,mkfs=reiserfs\n");
        msgprintf(MSG_FORCE, " * \e[1mrestore a filesystem from an archive and specify extra mkfs options:\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver restfs /data/myarchive1.fsa id=0,dest=/dev/sda1,mkfs=ext4,mkfsopt=\"-I 256\"\n");
        msgprintf(MSG_FORCE, " * \e[1mrestore a filesystem from an archive and specify a new label and a new UUID:\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver restfs /data/myarchive1.fsa id=0,dest=/dev/sda1,label=root,uuid=5f6e5f4f-dc2a-4dbd-a6ea-9ca997cde75e\n");
        msgprintf(MSG_FORCE, " * \e[1msave the contents of /usr/src/linux to an archive (similar to tar):\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver savedir /data/linux-sources.fsa /usr/src/linux\n");
        msgprintf(MSG_FORCE, " * \e[1msave a filesystem (/dev/sda1) to an archive split into volumes of 680MB:\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver savefs -s 680 /data/myarchive1.fsa /dev/sda1\n");
        msgprintf(MSG_FORCE, " * \e[1msave a filesystem and exclude all files/dirs called 'pagefile.*':\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver savefs /data/myarchive.fsa /dev/sda1 --exclude='pagefile.*'\n");
        msgprintf(MSG_FORCE, " * \e[1mgeneric exclude for 'share' such as '/usr/share' and '/usr/local/share':\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver savefs /data/myarchive.fsa --exclude=share\n");
        msgprintf(MSG_FORCE, " * \e[1mabsolute exclude valid for '/usr/share' but not for '/usr/local/share':\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver savefs /data/myarchive.fsa --exclude=/usr/share\n");
        msgprintf(MSG_FORCE, " * \e[1msave a filesystem (/dev/sda1) to an encrypted archive:\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver savefs -c mypassword /data/myarchive1.fsa /dev/sda1\n");
        msgprintf(MSG_FORCE, " * \e[1msame as before but prompt for password in the terminal:\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver savefs -c - /data/myarchive1.fsa /dev/sda1\n");
        msgprintf(MSG_FORCE, " * \e[1mextract an archive made of simple files to /tmp/extract:\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver restdir /data/linux-sources.fsa /tmp/extract\n");
        msgprintf(MSG_FORCE, " * \e[1mshow information about an archive and its filesystems:\e[0m\n");
        msgprintf(MSG_FORCE, "   fsarchiver archinfo /data/myarchive2.fsa\n");
    }
}

static struct option const long_options[] =
{
    {"allow-rw-mounted", no_argument, NULL, 'A'},
    {"allow-no-acl-xattr", no_argument, NULL, 'a'},
    {"buffer_size_read", required_argument, NULL, 'B'},
    {"cryptpass", required_argument, NULL, 'c'},
    {"debug", no_argument, NULL, 'd'},
    {"exclude", required_argument, NULL, 'e'},
    {"help", no_argument, NULL, 'h'},
    {"jobs_pfreader", required_argument, NULL, 'J'},
    {"jobs", required_argument, NULL, 'j'},
    {"label", required_argument, NULL, 'L'},
    {"maxmemcache", required_argument, NULL, 'M'},
    {"overwrite", no_argument, NULL, 'o'},
    {"split", required_argument, NULL, 's'},
    {"version", no_argument, NULL, 'V'},
    {"verbose", no_argument, NULL, 'v'},
    {"experimental", no_argument, NULL, 'x'},
    {"zstd", required_argument, NULL, 'Z'},
    {"compress", required_argument, NULL, 'z'},
    {NULL, 0, NULL, 0}
};

int process_cmdline(int argc, char **argv)
{
    char *partition[FSA_MAX_FSPERARCH];
    bool runasroot=true;
    char *probemode;
    sigset_t mask_set;
    bool probedetailed=0;
    char *command=NULL;
    char *archive=NULL;
    char tempbuf[1024];
    char *progname;
    int fscount;
    int argcok;
    int ret=0;
    int cmd;
    int c;

    // init
    memset(partition, 0, sizeof(partition));
    progname=argv[0];

    // set default options
    g_options.overwrite=false;
    g_options.allowsaverw=false;
    g_options.experimental=false;
    g_options.dontcheckmountopts=false;
    g_options.verboselevel=0;
    g_options.debuglevel=0;
    g_options.jobs_pfreader=FSA_DEF_THREAD_OPEN_FD;
    g_options.buffer_size_read=FSA_DEF_THREAD_READ_BUFSIZE;
    g_options.buffer_size_read_bytes=FSA_DEF_THREAD_READ_BUFSIZE * 1048576;
    g_options.compressjobs=1;
    g_options.datablocksize=FSA_DEF_BLKSIZE;
    g_options.encryptalgo=ENCRYPT_NONE;
    snprintf(g_options.archlabel, sizeof(g_options.archlabel), "<none>");
    g_options.encryptpass[0]=0;
    g_options.maxcacheblocks = (s64) FSA_MAX_QUEUESIZE;
    g_options.maxcachemem = (u64) 0;

    // set default compression mode
    g_options.compressalgo=FSA_DEF_COMPRESS_ALGO;
    g_options.compresslevel=FSA_DEF_COMPRESS_LEVEL;
#ifdef OPTION_ZSTD_SUPPORT
    g_options.fsacomplevel=FSA_DEF_COMPRESS_LEVEL;
#else
    g_options.fsacomplevel=FSA_DEF_FSACOMP_LEVEL;
#endif // OPTION_ZSTD_SUPPORT

    while ((c = getopt_long(argc, argv, "AaB:c:de:hJ:j:L:M:os:VvxZ:z:", long_options, NULL)) != EOF)
    {
        switch (c)
        {
            case 'o': // overwrite existing archives
                g_options.overwrite=true;
                break;
            case 'A': // allows to backup read/write mounted partition
                g_options.allowsaverw=true;
                break;
            case 'a': // don't check the mount options for already-mounted filesystems
                g_options.dontcheckmountopts=true;
                break;
            case 'B': // set the size of the per-thread read() buffer for savefs and savedir
                g_options.buffer_size_read = atoi(optarg);
                if (g_options.buffer_size_read<1 || g_options.buffer_size_read>FSA_MAX_THREAD_READ_BUFSIZE)
                {
                    errprintf("[%s] is not a valid PER-THREAD read() buffer size in MB. Must be between 1 and %d\n", optarg, FSA_MAX_THREAD_READ_BUFSIZE);
                    usage(progname, false);
                    return 1;
                }
                break;
            case 'c': // encryption
                g_options.encryptalgo=ENCRYPT_BLOWFISH;
                if ((strlen(optarg)<FSA_MIN_PASSLEN || strlen(optarg)>FSA_MAX_PASSLEN) && strcmp(optarg, "-")!=0)
                {   errprintf("the password lenght is incorrect, it must between %d and %d chars, or \"-\" for interactive password prompt.\n", FSA_MIN_PASSLEN, FSA_MAX_PASSLEN);
                    usage(progname, false);
                    return -1;
                }
                snprintf((char*)g_options.encryptpass, FSA_MAX_PASSLEN, "%s", optarg);
                break;
            case 'd': // debug mode
                g_options.debuglevel++;
                break;
            case 'e': // exclude files/directories
                strlist_add(&g_options.exclude, optarg);
                break;
            case 'h': // help
                usage(progname, true);
                return 0;
            case 'J': // file reader/writer jobs
                g_options.jobs_pfreader=atoi(optarg);
                if (g_options.jobs_pfreader<1 || g_options.jobs_pfreader>FSA_MAX_THREAD_OPEN_FD)
                {
                    errprintf("[%s] is not a valid number of jobs. Must be between 1 and %d\n", optarg, FSA_MAX_THREAD_OPEN_FD);
                    usage(progname, false);
                    return 1;
                }
                break;
            case 'j': // compression jobs
                g_options.compressjobs=atoi(optarg);
                if (g_options.compressjobs<1 || g_options.compressjobs>FSA_MAX_COMPJOBS)
                {
                    errprintf("[%s] is not a valid number of jobs. Must be between 1 and %d\n", optarg, FSA_MAX_COMPJOBS);
                    usage(progname, false);
                    return 1;
                }
                break;
            case 'L': // archive label
                snprintf(g_options.archlabel, sizeof(g_options.archlabel), "%s", optarg);
                break;
            case 'M': // Maximum block caching memory allocation in MB
                g_options.maxcachemem=((u64)atoll(optarg));
                break;
            case 's': // split archive into several volumes
                g_options.splitsize=((u64)atoll(optarg))*((u64)1024LL*1024LL);
                if (g_options.splitsize==0)
                {
                    errprintf("argument of option -s is invalid (%s). It must be a valid integer\n", optarg);
                    usage(progname, false);
                    return -1;
                }
                else // show the calculated size since it was probably incorrect before due to an integer overflow
                {
                    msgprintf(MSG_FORCE, "Archive will be split into volumes of %lld bytes (%s)\n",
                        (long long)g_options.splitsize, format_size(g_options.splitsize, tempbuf, sizeof(tempbuf), 'h'));
                }
                break;
            case 'V': // version
                msgprintf(MSG_FORCE, "fsarchiver %s (%s)\n", FSA_VERSION, FSA_RELDATE);
                return 0;
            case 'v': // verbose mode
                g_options.verboselevel++;
                break;
            case 'x': // enable support for experimental features
                g_options.experimental=true;
                break;
            case 'Z': // zstd compression level
#ifdef OPTION_ZSTD_SUPPORT
                g_options.compresslevel=atoi(optarg);
                if (g_options.compresslevel == 0) {
                    g_options.compressalgo=COMPRESS_NONE;
                    g_options.fsacomplevel=COMPRESS_LZ4;
                } else {
                    g_options.compressalgo=COMPRESS_ZSTD;
                    g_options.fsacomplevel=atoi(optarg);
                }
                if (g_options.compresslevel<0 || g_options.compresslevel>22)
                {   errprintf("[%s] is not a valid compression level, it must be an integer between 0 and 22.\n", optarg);
                    usage(progname, false);
                    return -1;
                }
                g_options.datablocksize=(g_options.compresslevel <= 19)?(FSA_DEF_BLKSIZE):(FSA_MAX_BLKSIZE);
                if (g_options.compresslevel>=20)
                {   msgprintf(MSG_FORCE, "Compression levels >= 20 may require a huge amount of memory\n"
                        "Please read the man page or \"http://www.fsarchiver.org/Compression\" for more details.\n");
                }
#else
                errprintf("zstd compression is not available as its support has been disabled at compilation time\n");
                return -1;
#endif // OPTION_ZSTD_SUPPORT
                break;
            case 'z': // legacy compression level
                g_options.fsacomplevel=atoi(optarg);
                if (g_options.fsacomplevel<0 || g_options.fsacomplevel>9)
                {   errprintf("[%s] is not a valid compression level, it must be an integer between 0 and 9.\n", optarg);
                    usage(progname, false);
                    return -1;
                }
                if (options_select_compress_level(g_options.fsacomplevel)<0)
                    return -1;
#ifdef OPTION_ZSTD_SUPPORT
                msgprintf(MSG_FORCE, "Legacy compression methods (-z) are deprecated.\n"
                    "It is recommended to switch to zstd using the -Z option.\n"
                    "Please read \"http://www.fsarchiver.org/Compression\" for more details.\n");
#endif // OPTION_ZSTD_SUPPORT
                if (g_options.fsacomplevel>=8)
                    msgprintf(MSG_FORCE, "Compression levels >= 8 may require a huge amount of memory\n"
                        "Please read the man page or \"http://www.fsarchiver.org/Compression\" for more details.\n");
                break;
            default:
                usage(progname, false);
                return -1;
        }
    }

    argc -= optind;
    argv += optind;

    // in all cases we need at least 1 parameters
    if (argc < 1)
    {   fprintf(stderr, "No arguments provided, cannot continue\n");
        usage(progname, false);
        return -1;
    }
    else // mandatory and unique parameters
    {
        command=*argv++, argc--;
    }

    int memres;
    int memfd;
    char memdatabuf[2048];
    char *memszstr=NULL;
    int memszstart=0;
    int memszend=0;
    u64 memsztemp=0;
    u64 memreal=0;
    bool memerr=false;
    
    memset(memdatabuf, 0, sizeof(memdatabuf));
    // The checksum will be in the obj-header not in a file footer
    if (((memfd=open("/proc/meminfo", O_RDONLY))<0) ||
        ((memres=read(memfd, memdatabuf, sizeof(memdatabuf)-1 ))<16)) {   
        errprintf("Cannot open /proc/meminfo for reading memory size\n");
        memerr=true;
        if (memfd >= 0) close(memfd);
    } else { 
        close(memfd);
        if (((memszstr=strcasestr(memdatabuf,"MemTotal:")) == NULL) ||
            ((memszstart=strspn(memszstr+9," \t")) < 1) ||
            ((memszend=strspn(memszstr+9+memszstart,"0123456789")) < 1)) {
               errprintf("Cannot find MemTotal value in /proc/meminfo\n");
               memerr=true;
        } else {
             *(memszstr + 9 + memszstart + memszend)='\0';
             char memunit = *(memszstr + 9 + memszstart + memszend + 1);
             memsztemp = (u64)atoll(memszstr+9+memszstart);
             if (memsztemp < 1) memsztemp = 1;
             switch (memunit) {
                 case 'b':
                 case 'B':
                     memreal = memsztemp / (u64) 1048576LL;
                     break;
                 case 'g':
                 case 'G':
                     memreal = memsztemp * (u64) 1024LL;
                     break;
                 case 'm':
                 case 'M':
                     memreal = memsztemp;
                     break;
                 case 'k':
                 case 'K':
                 default: 
                     memreal = memsztemp / (u64)1024LL;
                     break;
             }
        }
        if (memreal < 1) {
              msgprintf(MSG_FORCE, "System real ram computing as less than 1MB?!\n");
              memerr=true;
        }
    }
    if (memerr) {
        errprintf("Ignoring -M, -J, and -B command-line arguments\n");
        g_options.maxcachemem = 0;
        g_options.jobs_pfreader=FSA_DEF_THREAD_OPEN_FD;
        g_options.buffer_size_read=FSA_DEF_THREAD_READ_BUFSIZE;
    } else {
        // calculate max size of queue, based upon -M, -B, or -J options
        if (g_options.maxcachemem > 0) {
            if (memreal < g_options.maxcachemem) {
                 msgprintf(MSG_FORCE, "System real ram %ldMB < -M request of %ldMB\n", memreal, g_options.maxcachemem);
                 g_options.maxcachemem = memreal / 2;
                 msgprintf(MSG_FORCE, "Setting -M request to %ldMB (1/2 of real memory)\n",g_options.maxcachemem);
            } else {
                 u64 mbtemp1;
                 u64 mbtemp2;
                 mbtemp1 = (g_options.maxcachemem * (u64) 1045876);
                 mbtemp2 = (s64) (mbtemp1 / g_options.datablocksize);
                 msgprintf(MSG_FORCE, "System real ram calculated as %ldMB\n",memreal);
                 msgprintf(MSG_FORCE, "Implemented -M requested block cache of size %ldMB\n",g_options.maxcachemem);
                 g_options.maxcacheblocks = mbtemp2;
                 queue_resize(&g_queue, g_options.maxcacheblocks);
            }
        }
        //Ensure there's at least 32MB of real memory left after all planed allocations...
        if ( (g_options.jobs_pfreader * g_options.buffer_size_read) + g_options.maxcachemem > memreal -32 ) {
            int tempint = (FSA_DEF_THREAD_OPEN_FD * FSA_DEF_THREAD_READ_BUFSIZE) + g_options.maxcachemem;
            if (tempint > memreal-32) { // not enough mem for default -J, -B options with selected -M value
                if (tempint - g_options.maxcachemem > memreal - 32) { //not even with default -M value?!!!!!
                    msgprintf(MSG_FORCE, "Not enough real memory for 32MB for OS + default -M, -J, -B values?!\n");
                    msgprintf(MSG_FORCE, "Setting -M, -J, -B and wishing you Good Luck running this...");
                    g_options.maxcachemem = 0;
                } else {
                    msgprintf(MSG_FORCE, "Not enough real memory for 32MB for OS + default -J, -B values with -M=%ld\n", g_options.maxcachemem);
                    g_options.maxcachemem = memreal-32 - (FSA_DEF_THREAD_OPEN_FD * FSA_DEF_THREAD_READ_BUFSIZE);
                    msgprintf(MSG_FORCE, "Setting -J, -B to default values and -M to %ldMB", g_options.maxcachemem);
                }
            } else {
                msgprintf(MSG_FORCE, "Setting -J, -B to default values and keeping -M at %ldMB", g_options.maxcachemem);
            }
            g_options.jobs_pfreader=FSA_DEF_THREAD_OPEN_FD;
            g_options.buffer_size_read=FSA_DEF_THREAD_READ_BUFSIZE;
        } else {
            msgprintf(MSG_FORCE, "Implemented -B requested PER-THREAD read cache of size %dMB\n",g_options.buffer_size_read);
            msgprintf(MSG_FORCE, "Implemented -J requested file open64() read threads: %d\n",g_options.jobs_pfreader);

        }
        g_options.buffer_size_read_bytes = g_options.buffer_size_read * 1048576;
    }

    // calculate threshold for small files that are compressed together
    g_options.smallfilethresh=min(g_options.datablocksize/4, FSA_MAX_SMALLFILESIZE);
    msgprintf(MSG_DEBUG1, "Files smaller than %ld will be packed with other small files\n", (long)g_options.smallfilethresh);

    // convert commands to integers
    if (strcmp(command, "savefs")==0)
    {   cmd=OPER_SAVEFS;
        runasroot=true;
        argcok=(argc>=2);
    }
    else if (strcmp(command, "restfs")==0)
    {   cmd=OPER_RESTFS;
        runasroot=true;
        argcok=(argc>=2);
    }
    else if (strcmp(command, "savedir")==0)
    {   cmd=OPER_SAVEDIR;
        runasroot=true;
        argcok=(argc>=2);
    }
    else if (strcmp(command, "restdir")==0)
    {   cmd=OPER_RESTDIR;
        runasroot=true;
        argcok=(argc==2);
    }
    else if (strcmp(command, "archinfo")==0)
    {   cmd=OPER_ARCHINFO;
        runasroot=false;
        argcok=(argc==1);
    }
    else if (strcmp(command, "probe")==0)
    {   cmd=OPER_PROBE;
        runasroot=true;
        argcok=(argc<=1);
    }
    else // command not found
    {   errprintf("[%s] is not a valid command.\n", command);
        usage(progname, false);
        return -1;
    }

    // check there are enough parameters on the cmd line
    if (argcok!=true)
    {   errprintf("invalid number of arguments.\n");
        usage(progname, false);
        return -1;
    }

    // check if must be run as root
    if (runasroot==true && geteuid()!=0)
    {   errprintf("\"fsarchiver %s\" must be run as root. cannot continue.\n", command);
        return -1;
    }

    // interactive password
    if (strcmp((char*)g_options.encryptpass, "-")==0)
    {
        int passconfirm;
        char *passtmp=NULL;

        passconfirm = (cmd==OPER_SAVEFS || cmd==OPER_SAVEDIR);
        if ((passtmp=getpass("Enter password: "))==NULL)
        {   errprintf("failed to get interactive password from the console\n");
            return -1;
        }
        if (strlen(passtmp)<FSA_MIN_PASSLEN || strlen(passtmp)>FSA_MAX_PASSLEN)
        {   errprintf("the password lenght is incorrect, it must between %d and %d chars\n", FSA_MIN_PASSLEN, FSA_MAX_PASSLEN);
            return -1;
        }
        snprintf((char*)g_options.encryptpass, FSA_MAX_PASSLEN, "%s", passtmp);

        if (passconfirm==true)
        {
            if ((passtmp=getpass("Confirm password: "))==NULL)
            {   errprintf("failed to get interactive password from the console\n");
                return -1;
            }
            if (strcmp((char*)g_options.encryptpass, passtmp)!=0)
            {   errprintf("the passwords do not match\n");
                return -1;
            }
        }
    }

    // commands that require an archive as the first argument
    switch (cmd)
    {
        case OPER_SAVEFS:
        case OPER_RESTFS:
        case OPER_SAVEDIR:
        case OPER_RESTDIR:
        case OPER_ARCHINFO:
            archive=*argv++, argc--;
            break;
        case OPER_PROBE:
            probedetailed=false; // do "simple" by default
            if (g_options.verboselevel>0)
                probedetailed=true;
            if (argc>0)
            {   probemode=*argv++, argc--;
                if (strcmp(probemode, "detailed")==0)
                    probedetailed=true;
            }
            break;
    }

    // list of partitions to backup/restore
    for (fscount=0; (fscount < argc) && (argv[fscount]); fscount++)
        partition[fscount]=argv[fscount];

    // install signal handlers
    sigemptyset(&mask_set);
    sigaddset(&mask_set, SIGINT);
    sigaddset(&mask_set, SIGTERM);
    sigprocmask(SIG_SETMASK, &mask_set, NULL);

    if (g_options.debuglevel>0)
        logfile_open();

    switch (cmd)
    {
        case OPER_SAVEFS:
            ret=oper_save(archive, fscount, partition, ARCHTYPE_FILESYSTEMS);
            break;
        case OPER_SAVEDIR:
            ret=oper_save(archive, fscount, partition, ARCHTYPE_DIRECTORIES);
            break;
        case OPER_RESTFS:
        case OPER_RESTDIR:
        case OPER_ARCHINFO:
            ret=oper_restore(archive, fscount, partition, cmd);
            break;
        case OPER_PROBE:
            ret=oper_probe(probedetailed);
            break;
        default:
            errprintf("[%s] is not a valid command.\n", command);
            usage(progname, false);
            ret=-1;
            break;
    };

    logfile_close();

    return ret;
}

int main(int argc, char **argv)
{
    int ret;

    // init the lzo library
#ifdef OPTION_LZO_SUPPORT
    if (lzo_init() != LZO_E_OK)
    {   errprintf("internal error - lzo_init() failed\n");
        exit(EXIT_FAILURE);
    }
#endif // OPTION_LZO_SUPPORT

    // init libgcrypt
    if (crypto_init()!=0)
    {   errprintf("cannot initialize the crypto environment\n");
        exit(EXIT_FAILURE);
    }

    // init
    options_init();
    queue_init(&g_queue, FSA_MAX_QUEUESIZE);

    // bulk of the program
    ret=process_cmdline(argc, argv);

    // cleanup
    queue_destroy(&g_queue);
    options_destroy();

    // cleanup libgcrypt
    crypto_cleanup();

    return !!ret;
}
