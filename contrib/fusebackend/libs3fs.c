#include "postgres.h"
#include "storage/filesystem.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// for fread(), fwrite(), fseek(), ftell()
#include <stdio.h>

// for [dis]connect, aka [u]mount()
#include <sys/mount.h>

/* Do the module magic dance */
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(s3_connect);
PG_FUNCTION_INFO_V1(s3_disconnect);
PG_FUNCTION_INFO_V1(s3_open);
PG_FUNCTION_INFO_V1(s3_sync);
PG_FUNCTION_INFO_V1(s3_close);
PG_FUNCTION_INFO_V1(s3_mkdir);
PG_FUNCTION_INFO_V1(s3_remove);
PG_FUNCTION_INFO_V1(s3_chmod);

PG_FUNCTION_INFO_V1(s3_fread);
PG_FUNCTION_INFO_V1(s3_fwrite);
PG_FUNCTION_INFO_V1(s3_fseek);
PG_FUNCTION_INFO_V1(s3_ftell);

PG_FUNCTION_INFO_V1(s3_truncate);

PG_FUNCTION_INFO_V1(s3_fstat);
PG_FUNCTION_INFO_V1(s3_free_fstat);

Datum s3_connect(PG_FUNCTION_ARGS);
Datum s3_disconnect(PG_FUNCTION_ARGS);
Datum s3_open(PG_FUNCTION_ARGS);
Datum s3_sync(PG_FUNCTION_ARGS);
Datum s3_close(PG_FUNCTION_ARGS);
Datum s3_mkdir(PG_FUNCTION_ARGS);
Datum s3_remove(PG_FUNCTION_ARGS);
Datum s3_chmod(PG_FUNCTION_ARGS);

Datum s3_fread(PG_FUNCTION_ARGS);
Datum s3_fwrite(PG_FUNCTION_ARGS);
Datum s3_fseek(PG_FUNCTION_ARGS);
Datum s3_ftell(PG_FUNCTION_ARGS);

Datum s3_truncate(PG_FUNCTION_ARGS);

Datum s3_fstat(PG_FUNCTION_ARGS);
Datum s3_free_fstat(PG_FUNCTION_ARGS);

/*
struct HdfsFileInternalWrapper {
    //public:
        HdfsFileInternalWrapper() :
            input(true), stream(NULL) {
        }

        ~HdfsFileInternalWrapper() {
            if (input) {
                //delete static_cast<InputStream *>(stream);
            } else {
                //delete static_cast<OutputStream *>(stream);
            }
        }

        FILE * getInputStream() {
            if (!input) {
                throw(-1);
                //THROW(Hdfs::HdfsException,
                //      "Internal error: file was not opened for read.");
            }

            if (!stream) {
                //THROW(Hdfs::HdfsIOException, "File is not opened.");
                throw(-2);
            }

            return stream;
        }
        FILE * getOutputStream() {
            if (input) {
                throw(-1);
                //THROW(Hdfs::HdfsException,
                //      "Internal error: file was not opened for write.");
            }

            if (!stream) {
                throw(-2);
                //THROW(Hdfs::HdfsIOException, "File is not opened.");
            }

            return stream;
        }

        bool isInput() const {
            return input;
        }

        void setInput(bool input) {
            this->input = input;
        }

        void setStream(FILE * stream) {
            this->stream = stream;
        }

    //private:
        bool input;
        FILE * stream;
};
*/

typedef struct fuseFile_ {
    char isInput;
    FILE* stream;
} fuseFile;

Datum 
s3_connect(PG_FUNCTION_ARGS /*FsysName protocol, char * host, uint16_t port, char *ccname, void *token*/ ) 
{

    // Function should ensure s3fs is mounted, maybe checking for some file inside the 
    // mount point, and performing the mount if it's not there
    // (this likely depends on a symlink from /sbin/mount.s3fs to /opt/s3fs/bin/s3fs)

    char *host = NULL;
    int port = 0;
    hdfsFS fsObj = NULL;
    int retval = 0;
    void *token = NULL;
    char *ccname = NULL;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_connect outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    host = FSYS_UDF_GET_HOST(fcinfo);
    port = FSYS_UDF_GET_PORT(fcinfo);

    /* Kerberos token used to authenticate */
    token = FSYS_UDF_GET_TOKEN(fcinfo);

    /* Kerberos ticket cache path */
    ccname = FSYS_UDF_GET_CCNAME(fcinfo);

    if (NULL == host) {
        elog(WARNING, "get host invalid in s3_connect");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }


    /*
    int mount(const char *source, const char *target,
              const char *filesystemtype, unsigned long mountflags,
              const void *data);
    */


    // TODO
    // fsObj =  fuseMountBuilder(builder);

    if (NULL == fsObj) {
        retval = -1;
    }
    FSYS_UDF_SET_HDFS(fcinfo, fsObj);

    PG_RETURN_INT32(retval);

}


Datum 
s3_disconnect(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFS fileSystem*/) 
{

    int retval = 0;
    hdfsFS fsObj = NULL;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_disconnect outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    fsObj = FSYS_UDF_GET_HDFS(fcinfo);

    if (NULL == fsObj) {
        elog(WARNING, "get hdfs invalid in s3_disconnect");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    // Function should try to unmount the s3fs mountpoint

    retval = umount("/mnt/s3/hawq" /* const char *target*/);

    PG_RETURN_INT32(retval);

}


Datum 
s3_open(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFS fileSystem, char * path, int flags,
                      int bufferSize, short replication, int64_t blocksize */) 
{
    int retval = 0;
    hdfsFS fsObj = NULL;
    char *path = NULL;
    int flags = 0;
    int bufferSize = 0;
    short rep = 0;
    int64_t blocksize = 0;
    fuseFile * hFile = NULL;
    int numRetry = 300;
    long sleepTime = 0; //micro seconds
    const long maxSleep = 1 * 1000 * 1000; //1 seconds

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_openfile outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    fsObj = FSYS_UDF_GET_HDFS(fcinfo);
    path = FSYS_UDF_GET_PATH(fcinfo);
    flags = FSYS_UDF_GET_FILEFLAGS(fcinfo);
    bufferSize = FSYS_UDF_GET_FILEBUFSIZE(fcinfo);
    rep = FSYS_UDF_GET_FILEREP(fcinfo);
    blocksize = FSYS_UDF_GET_FILEBLKSIZE(fcinfo);

    if (NULL == fsObj) {
        elog(WARNING, "get fsObj invalid in s3_open");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (NULL == path || '\0' == *path) {
        elog(WARNING, "get path invalid in s3_openfile");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (bufferSize < 0 || rep < 0 || blocksize < 0) {
        elog(WARNING, "get param error in s3_openfile: bufferSize[%d], rep[%d], blocksize["INT64_FORMAT"]",
             bufferSize, rep, blocksize);
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    do {
        if (sleepTime > 0) {
            pg_usleep(sleepTime);
        }


        // libhdfs3 implementation
        // can safely ignore bufferSize, replication, blocksize

        // TODO Need to check flags for CREATE, APPEND, or WRITE -> hFile.setInput(false);
        // map flags code -> mode string


        const char* mode = "w+";

        FILE * stream = fopen(path, mode);

        hFile = (fuseFile*)malloc(sizeof(fuseFile));
        hFile->isInput = 0;
        hFile->stream = stream;

        /// -----------

        sleepTime = sleepTime * 2 + 10000;
        sleepTime = sleepTime < maxSleep ? sleepTime : maxSleep;
    } while (--numRetry > 0 && hFile == NULL && errno == EBUSY);

    if (NULL == hFile) {
        retval = -1;
    }
    FSYS_UDF_SET_HFILE(fcinfo, hFile);

    PG_RETURN_INT32(retval);
}

Datum 
s3_sync(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFS fileSystem, hdfsFile file*/) 
{
    int retval = 0;
    hdfsFS fsObj = NULL;
    fuseFile *hFile = NULL;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_sync outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    fsObj = FSYS_UDF_GET_HDFS(fcinfo);
    hFile = FSYS_UDF_GET_HFILE(fcinfo);
    if (NULL == fsObj) {
        elog(WARNING, "get fsObj invalid in s3_sync");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (NULL == hFile) {
        elog(WARNING, "get fuseFile invalid in s3_sync");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    //
    FILE * fd = hFile->stream;
    retval = fflush(fd);
    ///

    PG_RETURN_INT32(retval);

}

Datum 
s3_close(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFS fileSystem, fuseFile file*/) 
{

    int retval = 0;
    hdfsFS fsObj = NULL;
    fuseFile *hFile = NULL;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_closefile outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    fsObj = FSYS_UDF_GET_HDFS(fcinfo);
    hFile = FSYS_UDF_GET_HFILE(fcinfo);
    if (NULL == fsObj) {
        elog(WARNING, "get hdfsFS invalid in s3_closefile");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (NULL == hFile) {
        elog(WARNING, "get fuseFile invalid in s3_closefile");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    FILE * fd = hFile->stream;
    retval = fclose(fd);
    // TODO check stuff

    free(hFile);

    PG_RETURN_INT32(retval);
}

Datum 
s3_mkdir(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFS fileSystem, char * path*/) 
{
    int retval = 0;
    hdfsFS fsObj = NULL;
    char *path = NULL;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_createdirectory outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    fsObj = FSYS_UDF_GET_HDFS(fcinfo);
    path = FSYS_UDF_GET_PATH(fcinfo);
    if (NULL == fsObj) {
        elog(WARNING, "get hdfsFS invalid in s3_createdirectory");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (NULL == path) {
        elog(WARNING, "get path invalid in s3_createdirectory");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    // libhdfs3 hardcodes 755
    retval = mkdir(path, 755);

    PG_RETURN_INT32(retval);
}

Datum 
s3_remove(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFS fileSystem, char * path, int recursive*/)
{
    int retval = 0;
    hdfsFS fsObj = NULL;
    char *path = NULL;
    int recursive = 0;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_delete outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    fsObj = FSYS_UDF_GET_HDFS(fcinfo);
    path = FSYS_UDF_GET_PATH(fcinfo);
    recursive = FSYS_UDF_GET_RECURSIVE(fcinfo);
    if (NULL == fsObj) {
        elog(WARNING, "get hdfsFS invalid in s3_delete");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (NULL == path) {
        elog(WARNING, "get path invalid in s3_delete");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    // Not sure there's a simple way to do recursive delete via API
    if (recursive) {
        retval = -1;
    }
    else {
        retval = remove(path);
    }

    PG_RETURN_INT32(retval);

}

Datum 
s3_chmod(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFS fileSystem, char * path, short mode */) 
{

    int retval = 0;
    hdfsFS fsObj = NULL;
    char *path = NULL;
    short mode = 0;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_chmod outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    fsObj = FSYS_UDF_GET_HDFS(fcinfo);
    path = FSYS_UDF_GET_PATH(fcinfo);
    mode = FSYS_UDF_GET_MODE(fcinfo);
    if (NULL == fsObj) {
        elog(WARNING, "get hdfsFS invalid in s3_chmod");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (NULL == path) {
        elog(WARNING, "get path invalid in s3_chmod");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    retval = chmod(path, mode);

    PG_RETURN_INT32(retval);
}

Datum 
s3_fread(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFS fileSystem, hdfsFile file, void * buffer, int length */) 
{

    int retval = 0;
    hdfsFS fsObj = NULL;
    fuseFile * hFile = NULL;
    char *buf = NULL;
    int length = 0;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_read outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    fsObj = FSYS_UDF_GET_HDFS(fcinfo);
    hFile = FSYS_UDF_GET_HFILE(fcinfo);
    buf = FSYS_UDF_GET_DATABUF(fcinfo);
    length = FSYS_UDF_GET_BUFLEN(fcinfo);
    if (NULL == fsObj) {
        elog(WARNING, "get hdfsFS invalid in s3_read");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (NULL == hFile) {
        elog(WARNING, "get fuseFile invalid in s3_read");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (NULL == buf) {
        elog(WARNING, "get buffer invalid in s3_read");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (length < 0) { /* TODO liugd: or <= 0 ? */
        elog(WARNING, "get length[%d] invalid in s3_read", length);
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }


    // TODO
    FILE * stream = hFile->stream;

    // Write to ptr buffer, size is the size of each element, nmemb is the length / numElements
    retval = (int) fread((void*) buf, (size_t) 1, (size_t) length, stream);

    PG_RETURN_INT32(retval);

}

Datum 
s3_fwrite(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFS fileSystem, hdfsFile file, const void * buffer, int length */) 
{

    int retval = 0;
    hdfsFS fsObj = NULL;
    fuseFile *hFile = NULL;
    char *buf = NULL;
    int length = 0;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_write outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    fsObj = FSYS_UDF_GET_HDFS(fcinfo);
    hFile = FSYS_UDF_GET_HFILE(fcinfo);
    buf = FSYS_UDF_GET_DATABUF(fcinfo);
    length = FSYS_UDF_GET_BUFLEN(fcinfo);
    if (NULL == fsObj) {
        elog(WARNING, "get hdfsFS invalid in s3_write");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (NULL == hFile) {
        elog(WARNING, "get fuseFile invalid in s3_write");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (NULL == buf) {
        elog(WARNING, "get buffer invalid in s3_write");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (length < 0) { /* TODO liugd: or <= 0 ? */
        elog(WARNING, "get length[%d] invalid in s3_write", length);
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    // TODO

    FILE * stream = hFile->stream;

    // Write from ptr buffer, size is the size of each element, nmemb is the length / numElements
    //return (int) fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream);

    const short oneByte = 1;

    retval = fwrite((void *)buf, (size_t)oneByte, (size_t)length, stream);

    PG_RETURN_INT32(retval);
}

Datum 
s3_fseek(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFS fileSystem, fuseFile file, int64_t desiredPos */) 
{

    int retval = 0;
    hdfsFS fsObj = NULL;
    fuseFile *hFile = NULL;
    int64_t pos = 0;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_seek outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT64(retval);
    }

    fsObj = FSYS_UDF_GET_HDFS(fcinfo);
    hFile = FSYS_UDF_GET_HFILE(fcinfo);
    pos = FSYS_UDF_GET_POS(fcinfo);
    if (NULL == fsObj) {
        elog(WARNING, "get hdfsFS invalid in s3_seek");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT64(retval);
    }
    if (NULL == hFile) {
        elog(WARNING, "get fuseFile invalid in s3_seek");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT64(retval);
    }
    if (pos < 0) {
        elog(WARNING, "get pos["INT64_FORMAT"] invalid in s3_seek", pos);
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT64(retval);
    }

    FILE * stream = hFile->stream;

    // Should be relative to start of the file
    retval = fseek(stream, (long) pos, SEEK_SET);

    PG_RETURN_INT64(retval);
}

Datum 
s3_ftell(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFS fileSystem, hdfsFile file */) 
{

    int64_t retval = 0;
    hdfsFS fsObj = NULL;
    fuseFile *hFile = NULL;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_tell outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT64(retval);
    }

    fsObj = FSYS_UDF_GET_HDFS(fcinfo);
    hFile = FSYS_UDF_GET_HFILE(fcinfo);
    if (NULL == fsObj) {
        elog(WARNING, "get hdfsFS invalid in s3_tell");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT64(retval);
    }
    if (NULL == hFile) {
        elog(WARNING, "get fuseFile invalid in s3_tell");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT64(retval);
    }

    FILE * stream = hFile->stream;

    retval = (int64_t) ftell(stream);

    PG_RETURN_INT64(retval);
}

Datum 
s3_truncate(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFS fileSystem, char * path, int64_t size */) 
{

    int retval = 0;
    hdfsFS fsObj = NULL;
    char *path = NULL;
    int64_t pos = 0;
    //int shouldWait;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_tell outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    fsObj = FSYS_UDF_GET_HDFS(fcinfo);
    path = FSYS_UDF_GET_PATH(fcinfo);
    pos = FSYS_UDF_GET_POS(fcinfo);
    if (NULL == fsObj) {
        elog(WARNING, "get hdfsFS invalid in s3_truncate");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (NULL == path) {
        elog(WARNING, "get fuseFile invalid in s3_truncate");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (pos < 0) {
        elog(WARNING, "get pos["INT64_FORMAT"] invalid in s3_truncate", pos);
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    retval = truncate(path, (off_t) pos);

    PG_RETURN_INT32(retval);
}

void
populateFileInfoFromStatStruct(struct stat * statInfo, char * name, hdfsFileInfo * fileInfo)
{

    fileInfo->mKind = S_ISDIR(statInfo->st_mode) ? (tObjectKind) 'D' : (tObjectKind) 'F' ;
    fileInfo->mName = name ;
    fileInfo->mLastMod = statInfo->st_mtime ;
    fileInfo->mSize = statInfo->st_size ;
    fileInfo->mReplication = 1;
    fileInfo->mBlockSize = 1;
    fileInfo->mOwner = " " ; /*TODO uid->name lookup statInfo->st_uid */
    fileInfo->mGroup = " " ; /* statInfo->st_gid */
    fileInfo->mPermissions = statInfo->st_mode ;
    fileInfo->mLastAccess = statInfo->st_atime ;
    //fileInfo->mHdfsEncryptionFileInfo = NULL;

}

void
buildFileInfoArray(char * path, hdfsFileInfo * fileInfo)
{
    struct stat st;

    lstat(path, &st);

    if (S_ISREG(st.st_mode)) {
        fileInfo = (hdfsFileInfo*) malloc(sizeof(hdfsFileInfo));
        populateFileInfoFromStatStruct(&st, path, fileInfo);
        return;
    }
    // TODO else we need to go recursive and malloc a bunch of stuff
    //
}

/*
static long goDir(char *dirname)
{
    DIR *dir = opendir(dirname);
    if (dir == 0)
        return 0;

    struct dirent *dit;
    struct stat st;
    long size = 0;
    long total_size = 0;
    char filePath[NAME_MAX];

    while ((dit = readdir(dir)) != NULL)
    {
        if ( (strcmp(dit->d_name, ".") == 0) || (strcmp(dit->d_name, "..") == 0) )
            continue;

        sprintf(filePath, "%s/%s", dirname, dit->d_name);
        if (lstat(filePath, &st) != 0)
            continue;
        size = st.st_size;

        if (S_ISDIR(st.st_mode))
        {
            long dir_size = goDir(filePath) + size;
            printf("DIR\t");
            printf("MODE: %lo\t", (unsigned long) st.st_mode);
            printf("SIZE: %ld\t", dir_size);
            printf("%s\n", filePath);
            total_size += dir_size;
        }
        else
        {
            total_size += size;
            printf("FILES\t");
            printf("MODE: %lo\t", (unsigned long) st.st_mode);
            printf("SIZE: %ld\t", size);
            printf("%s\n", filePath);
        }
    }
    return total_size;
}
*/

Datum 
s3_fstat(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFS fileSystem, char * path */) 
{
    int64_t retval = 0;
    hdfsFS fsObj = NULL;
    char *path = NULL;
    hdfsFileInfo *fileinfo = NULL;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_tell outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    fsObj = FSYS_UDF_GET_HDFS(fcinfo);
    path = FSYS_UDF_GET_PATH(fcinfo);
    if (NULL == fsObj) {
        elog(WARNING, "get hdfsFS invalid in s3_getpathinfo");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }
    if (NULL == path) {
        elog(WARNING, "get file path invalid in s3_getpathinfo");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT32(retval);
    }

    // TODO - need to handle file info for directories

    buildFileInfoArray(path, fileinfo);

    if (NULL == fileinfo) {
        retval = -1;
    }

    FSYS_UDF_SET_FILEINFO(fcinfo, fileinfo);

    PG_RETURN_INT64(retval);

}


Datum 
s3_free_fstat(PG_FUNCTION_ARGS /*FsysName protocol, hdfsFileInfo * hdfsFileInfo, int numEntries */) 
{
    int64_t retval = 0;
    hdfsFileInfo *fileinfo = NULL;
    int numEntries = 0;

    /* Must be called via the filesystem manager */
    if (!CALLED_AS_GPFILESYSTEM(fcinfo)) {
        elog(WARNING, "cannot execute s3_tell outside filesystem manager");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT64(retval);
    }

    fileinfo = FSYS_UDF_GET_FILEINFO(fcinfo);
    numEntries = FSYS_UDF_GET_FILEINFONUM(fcinfo);
    if (NULL == fileinfo) {
        elog(WARNING, "get hdfsFileInfo invalid in s3_freefileinfo");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT64(retval);
    }
    if (numEntries < 0) {
        elog(WARNING, "get hdfsFileInfo numEntries invalid in s3_freefileinfo");
        retval = -1;
        errno = EINVAL;
        PG_RETURN_INT64(retval);
    }

    // TODO - need to handle list of these
    if (numEntries > 1) {
        return -1;
    }
    else if (numEntries == 0) {
        return -1;
    }
    else {
        free(fileinfo);
    }

    PG_RETURN_INT64(retval);
    return -1;
}
