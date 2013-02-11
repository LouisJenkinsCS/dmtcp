/****************************************************************************
 *   Copyright (C) 2006-2010 by Jason Ansel, Kapil Arya, and Gene Cooperman *
 *   jansel@csail.mit.edu, kapil@ccs.neu.edu, gene@ccs.neu.edu              *
 *                                                                          *
 *   This file is part of the dmtcp/src module of DMTCP (DMTCP:dmtcp/src).  *
 *                                                                          *
 *  DMTCP:dmtcp/src is free software: you can redistribute it and/or        *
 *  modify it under the terms of the GNU Lesser General Public License as   *
 *  published by the Free Software Foundation, either version 3 of the      *
 *  License, or (at your option) any later version.                         *
 *                                                                          *
 *  DMTCP:dmtcp/src is distributed in the hope that it will be useful,      *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of          *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the           *
 *  GNU Lesser General Public License for more details.                     *
 *                                                                          *
 *  You should have received a copy of the GNU Lesser General Public        *
 *  License along with DMTCP:dmtcp/src.  If not, see                        *
 *  <http://www.gnu.org/licenses/>.                                         *
 ****************************************************************************/

#include <sys/ioctl.h>
#include <sys/select.h>
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <termios.h>
#include <iostream>
#include <ios>
#include <fstream>
#include <linux/limits.h>
#include <arpa/inet.h>

#include "dmtcpplugin.h"
#include "shareddata.h"
#include "util.h"
#include "jsocket.h"
#include "jassert.h"
#include "jfilesystem.h"
#include "jconvert.h"

#include "fileconnection.h"
#include "filewrappers.h"

static bool ptmxTestPacketMode(int masterFd);
static ssize_t ptmxReadAll(int fd, const void *origBuf, size_t maxCount);
static ssize_t ptmxWriteAll(int fd, const void *buf, bool isPacketMode);
static void CopyFile(const dmtcp::string& src, const dmtcp::string& dest);
static void CreateDirectoryStructure(const dmtcp::string& path);

#ifdef REALLY_VERBOSE_CONNECTION_CPP
static bool really_verbose = true;
#else
static bool really_verbose = false;
#endif

static bool _isVimApp()
{
  static int isVimApp = -1;

  if (isVimApp == -1) {
    dmtcp::string progName = jalib::Filesystem::GetProgramName();

    if (progName == "vi" || progName == "vim" || progName == "vim-normal" ||
        progName == "vim.basic"  || progName == "vim.tiny" ||
        progName == "vim.gtk" || progName == "vim.gnome") {
      isVimApp = 1;
    } else {
      isVimApp = 0;
    }
  }
  return isVimApp;
}

static bool _isBlacklistedFile(dmtcp::string& path)
{
  if ((dmtcp::Util::strStartsWith(path, "/dev/") &&
       !dmtcp::Util::strStartsWith(path, "/dev/shm/")) ||
      dmtcp::Util::strStartsWith(path, "/proc/") ||
      dmtcp::Util::strStartsWith(path, dmtcp_get_tmpdir())) {
    return true;
  }
  return false;
}

/*****************************************************************************
 * Pseudo-TTY Connection
 *****************************************************************************/

static bool ptmxTestPacketMode(int masterFd)
{
  char tmp_buf[100];
  int slave_fd, ioctlArg, rc;
  fd_set read_fds;
  struct timeval zeroTimeout = {0, 0}; /* Zero: will use to poll, not wait.*/

  _real_ptsname_r(masterFd, tmp_buf, 100);
  /* permissions not used, but _real_open requires third arg */
  slave_fd = _real_open(tmp_buf, O_RDWR, 0666);

  /* A. Drain master before testing.
     Ideally, DMTCP has already drained it and preserved any information
     about exceptional conditions in command byte, but maybe we accidentally
     caused a new command byte in packet mode. */
  /* Note:  if terminal was in packet mode, and the next read would be
     a non-data command byte, then there's no easy way for now to detect and
     restore this. ?? */
  /* Note:  if there was no data to flush, there might be no command byte,
     even in packet mode. */
  tcflush(slave_fd, TCIOFLUSH);
  /* If character already transmitted(usual case for pty), then this flush
     will tell master to flush it. */
  tcflush(masterFd, TCIFLUSH);

  /* B. Now verify that read_fds has no more characters to read. */
  ioctlArg = 1;
  ioctl(masterFd, TIOCINQ, &ioctlArg);
  /* Now check if there's a command byte still to read. */
  FD_ZERO(&read_fds);
  FD_SET(masterFd, &read_fds);
  select(masterFd + 1, &read_fds, NULL, NULL, &zeroTimeout);
  if (FD_ISSET(masterFd, &read_fds)) {
    // Clean up someone else's command byte from packet mode.
    // FIXME:  We should restore this on resume/restart.
    rc = read(masterFd, tmp_buf, 100);
    JASSERT(rc == 1) (rc) (masterFd);
  }

  /* C. Now we're ready to do the real test.  If in packet mode, we should
     see command byte of TIOCPKT_DATA(0) with data. */
  tmp_buf[0] = 'x'; /* Don't set '\n'.  Could be converted to "\r\n". */
  /* Give the masterFd something to read. */
  JWARNING((rc = write(slave_fd, tmp_buf, 1)) == 1) (rc) .Text("write failed");
  //tcdrain(slave_fd);
  _real_close(slave_fd);

  /* Read the 'x':  If we also see a command byte, it's packet mode */
  rc = read(masterFd, tmp_buf, 100);

  /* D. Check if command byte packet exists, and chars rec'd is longer by 1. */
  return(rc == 2 && tmp_buf[0] == TIOCPKT_DATA && tmp_buf[1] == 'x');
}

// Also record the count read on each iteration, in case it's packet mode
static bool readyToRead(int fd)
{
  fd_set read_fds;
  struct timeval zeroTimeout = {0, 0}; /* Zero: will use to poll, not wait.*/
  FD_ZERO(&read_fds);
  FD_SET(fd, &read_fds);
  select(fd + 1, &read_fds, NULL, NULL, &zeroTimeout);
  return FD_ISSET(fd, &read_fds);
}

// returns 0 if not ready to read; else returns -1, or size read incl. header
static ssize_t readOnePacket(int fd, const void *buf, size_t maxCount)
{
  typedef int hdr;
  ssize_t rc = 0;
  // Read single packet:  rc > 0 will be true for at most one iteration.
  while (readyToRead(fd) && rc <= 0) {
    rc = read(fd,(char *)buf+sizeof(hdr), maxCount-sizeof(hdr));
    *(hdr *)buf = rc; // Record the number read in header
    if (rc >=(ssize_t) (maxCount-sizeof(hdr))) {
      rc = -1; errno = E2BIG; // Invoke new errno for buf size not large enough
    }
    if (rc == -1 && errno != EAGAIN && errno != EINTR)
      break;  /* Give up; bad error */
  }
  return(rc <= 0 ? rc : rc+sizeof(hdr));
}

// rc < 0 => error; rc == sizeof(hdr) => no data to read;
// rc > 0 => saved w/ count hdr
static ssize_t ptmxReadAll(int fd, const void *origBuf, size_t maxCount)
{
  typedef int hdr;
  char *buf =(char *)origBuf;
  int rc;
  while ((rc = readOnePacket(fd, buf, maxCount)) > 0) {
    buf += rc;
  }
  *(hdr *)buf = 0; /* Header count of zero means we're done */
  buf += sizeof(hdr);
  JASSERT(rc < 0 || buf -(char *)origBuf > 0) (rc) (origBuf) ((void *)buf);
  return(rc < 0 ? rc : buf -(char *)origBuf);
}

// The hdr contains the size of the full buffer([hdr, data]).
// Return size of origBuf written:  includes packets of form:  [hdr, data]
//   with hdr holding size of data.  Last hdr has value zero.
// Also record the count written on each iteration, in case it's packet mode.
static ssize_t writeOnePacket(int fd, const void *origBuf, bool isPacketMode)
{
  typedef int hdr;
  int count = *(hdr *)origBuf;
  int cum_count = 0;
  int rc = 0; // Trigger JASSERT if not modified below.
  if (count == 0)
    return sizeof(hdr);  // count of zero means we're done, hdr consumed
  // FIXME:  It would be nice to restore packet mode(flow control, etc.)
  //         For now, we ignore it.
  if (count == 1 && isPacketMode)
    return sizeof(hdr) + 1;
  while (cum_count < count) {
    rc = write(fd,(char *)origBuf+sizeof(hdr)+cum_count, count-cum_count);
    if (rc == -1 && errno != EAGAIN && errno != EINTR)
      break;  /* Give up; bad error */
    if (rc >= 0)
      cum_count += rc;
  }
  JASSERT(rc != 0 && cum_count == count)
    (JASSERT_ERRNO) (rc) (count) (cum_count);
  return(rc < 0 ? rc : cum_count+sizeof(hdr));
}

static ssize_t ptmxWriteAll(int fd, const void *buf, bool isPacketMode)
{
  typedef int hdr;
  ssize_t cum_count = 0;
  ssize_t rc;
  while ((rc = writeOnePacket(fd,(char *)buf+cum_count, isPacketMode))
         >(ssize_t)sizeof(hdr)) {
    cum_count += rc;
  }
  JASSERT(rc < 0 || rc == sizeof(hdr)) (rc) (cum_count);
  cum_count += sizeof(hdr);  /* Account for last packet: 'done' hdr w/ 0 data */
  return(rc <= 0 ? rc : cum_count);
}

dmtcp::PtyConnection::PtyConnection(int fd, const char *path,
                                    int flags, mode_t mode, int type)
  : Connection (PTY)
  , _flags(flags)
  , _mode(mode)
{
  char buf[PTS_PATH_MAX];
  _type = type;
  switch (_type) {

    case PTY_DEV_TTY:
      _ptsName = path;
      break;

    case PTY_CTTY:
      _ptsName = path;
      SharedData::getVirtPtyName(path, buf, sizeof(buf));
      if (strlen(buf) == 0) {
        SharedData::createVirtualPtyName(path, buf, sizeof(buf));
      }
      _virtPtsName = buf;
      JTRACE("creating CTTY connection") (_ptsName) (_virtPtsName);

      break;

    case PTY_MASTER:
      _masterName = path;
      JASSERT(_real_ptsname_r(fd, buf, sizeof(buf)) == 0) (JASSERT_ERRNO);
      _ptsName = buf;

      // glibc allows only 20 char long buf
      // Check if there is enough room to insert the string "dmtcp_" before the
      //   terminal number, if not then we ASSERT here.
      JASSERT((strlen(buf) + strlen("v")) <= 20)
        .Text("string /dev/pts/<n> too long, can not be virtualized."
              "Once possible workarong here is to replace the string"
              "\"dmtcp_\" with something short like \"d_\" or even "
              "\"d\" and recompile DMTCP");

      // Generate new Unique buf
      SharedData::createVirtualPtyName(_ptsName.c_str(), buf, sizeof(buf));
      _virtPtsName = buf;
      JTRACE("creating ptmx connection") (_ptsName) (_virtPtsName);
      break;

    case PTY_SLAVE:
      _ptsName = path;
      SharedData::getVirtPtyName(path, buf, sizeof(buf));
      _virtPtsName = buf;
      JASSERT(strlen(buf) != 0) (path);
      JTRACE("creating pts connection") (_ptsName) (_virtPtsName);
      break;

    case PTY_BSD_MASTER:
      _masterName = path;
      break;

    case PTY_BSD_SLAVE:
      _ptsName = path;
      break;

    default:
      break;
  }
}

void dmtcp::PtyConnection::preCheckpoint()
{
  if (ptyType() == PTY_MASTER) {
    const int maxCount = 10000;
    char buf[maxCount];
    int numRead, numWritten;
    // _fds[0] is master fd
    numRead = ptmxReadAll(_fds[0], buf, maxCount);
    _ptmxIsPacketMode = ptmxTestPacketMode(_fds[0]);
    JTRACE("_fds[0] is master(/dev/ptmx)") (_fds[0]) (_ptmxIsPacketMode);
    numWritten = ptmxWriteAll(_fds[0], buf, _ptmxIsPacketMode);
    JASSERT(numRead == numWritten) (numRead) (numWritten);
  }
}

void dmtcp::PtyConnection::refill(bool isRestart)
{
  if (ptyType() == PTY_SLAVE || ptyType() == PTY_BSD_SLAVE) {
    JASSERT(_ptsName.compare("?") != 0);
    JTRACE("Restoring PTY slave") (_fds[0]) (_ptsName) (_virtPtsName);
    if (ptyType() == PTY_SLAVE) {
      char buf[32];
      SharedData::getRealPtyName(_virtPtsName.c_str(), buf, sizeof(buf));
      JASSERT(strlen(buf) > 0) (_virtPtsName) (_ptsName);
      _ptsName = buf;
    }

    int tempfd = _real_open(_ptsName.c_str(), O_RDWR);
    JASSERT(tempfd >= 0) (_virtPtsName) (_ptsName) (JASSERT_ERRNO)
      .Text("Error Opening PTS");

    JTRACE("Restoring PTS real") (_ptsName) (_virtPtsName) (_fds[0]);
    Util::dupFds(tempfd, _fds);
  }
  restoreOptions();
}

void dmtcp::PtyConnection::postRestart()
{
  JASSERT(_fds.size() > 0);
  if (ptyType() == PTY_SLAVE || ptyType() == PTY_BSD_SLAVE) {
    return;
  }

  int tempfd;

  switch (ptyType()) {
    case PTY_INVALID:
      //tempfd = _real_open("/dev/null", O_RDWR);
      JTRACE("Restoring invalid PTY.") (id());
      return;

    case PTY_DEV_TTY:
      {
        dmtcp::string tty = "/dev/tty";
        tempfd = _real_open(tty.c_str(), _fcntlFlags);
        JASSERT(tempfd >= 0) (tempfd) (tty) (JASSERT_ERRNO)
          .Text("Error Opening the terminal device");

        JTRACE("Restoring /dev/tty for the process") (tty) (_fds[0]);
        _ptsName = _virtPtsName = tty;
        break;
      }

    case PTY_CTTY:
      {
        dmtcp::string controllingTty = jalib::Filesystem::GetControllingTerm();
        dmtcp::string stdinDeviceName =
          (jalib::Filesystem::GetDeviceName(STDIN_FILENO));
        if (controllingTty.length() == 0) {
          JTRACE("Unable to restore terminal attached with the process.\n"
                 "Replacing it with current STDIN")
            (stdinDeviceName);
          JWARNING(Util::strStartsWith(stdinDeviceName, "/dev/pts/") ||
                   stdinDeviceName == "/dev/tty")
            .Text("Controlling terminal not bound to a terminal device.");
        }

        if (Util::isValidFd(STDIN_FILENO)) {
          tempfd = STDIN_FILENO;
        } else if (Util::isValidFd(STDOUT_FILENO)) {
          tempfd = STDOUT_FILENO;
        } else if (controllingTty.length() > 0) {
          tempfd = _real_open(controllingTty.c_str(), _fcntlFlags);
          JASSERT(tempfd >= 0) (tempfd) (controllingTty) (JASSERT_ERRNO)
            .Text("Error Opening the terminal attached with the process");
        } else {
          JASSERT("Controlling terminal and STDIN/OUT not found.");
        }

        JTRACE("Restoring CTTY for the process") (controllingTty) (_fds[0]);

        _ptsName = controllingTty;
        SharedData::insertPtyNameMap(_virtPtsName.c_str(), _ptsName.c_str());
        break;
      }

    case PTY_MASTER:
      {
        char pts_name[80];

        tempfd = _real_open("/dev/ptmx", O_RDWR);
        JASSERT(tempfd >= 0) (tempfd) (JASSERT_ERRNO)
          .Text("Error Opening /dev/ptmx");

        JASSERT(grantpt(tempfd) >= 0) (tempfd) (JASSERT_ERRNO);
        JASSERT(unlockpt(tempfd) >= 0) (tempfd) (JASSERT_ERRNO);
        JASSERT(_real_ptsname_r(tempfd, pts_name, 80) == 0)
          (tempfd) (JASSERT_ERRNO);

        _ptsName = pts_name;
        SharedData::insertPtyNameMap(_virtPtsName.c_str(), _ptsName.c_str());

        if (ptyType() == PTY_MASTER) {
          int packetMode = _ptmxIsPacketMode;
          ioctl(_fds[0], TIOCPKT, &packetMode); /* Restore old packet mode */
        }

        JTRACE("Restoring /dev/ptmx") (_fds[0]) (_ptsName) (_virtPtsName);
        break;
      }
    case PTY_BSD_MASTER:
      {
        JTRACE("Restoring BSD Master Pty") (_masterName) (_fds[0]);
        //dmtcp::string slaveDeviceName =
          //_masterName.replace(0, strlen("/dev/pty"), "/dev/tty");

        tempfd = _real_open(_masterName.c_str(), O_RDWR);

        // FIXME: If unable to open the original BSD Master Pty, we should try to
        // open another one until we succeed and then open slave device
        // accordingly.
        // This can be done by creating a function openBSDMaster, which will try
        // to open the original master device, but if unable to do so, it would
        // keep on trying all the possible BSD Master devices until one is
        // opened. It should then create a mapping between original Master/Slave
        // device name and current Master/Slave device name.
        JASSERT(tempfd >= 0) (tempfd) (JASSERT_ERRNO)
          .Text("Error Opening BSD Master Pty.(Already in use?)");
        break;
      }
    default:
      {
        // should never reach here
        JASSERT(false) .Text("Should never reach here.");
      }
  }
  Util::dupFds(tempfd, _fds);
}

void dmtcp::PtyConnection::serializeSubClass(jalib::JBinarySerializer& o)
{
  JSERIALIZE_ASSERT_POINT("dmtcp::PtyConnection");
  o & _ptsName & _virtPtsName & _masterName & _type & _ptmxIsPacketMode;
  JTRACE("Serializing PtyConn.") (_ptsName) (_virtPtsName);
}

/*****************************************************************************
 * File Connection
 *****************************************************************************/

// Upper limit on filesize for files that are automatically chosen for ckpt.
// Default 100MB
#define MAX_FILESIZE_TO_AUTOCKPT (100 * 1024 * 1024)

void dmtcp::FileConnection::doLocking()
{
  if (dmtcp::Util::strStartsWith(_path, "/proc/")) {
    int index = 6;
    char *rest;
    pid_t proc_pid = strtol(&_path[index], &rest, 0);
    if (proc_pid > 0 && *rest == '/') {
      _type = FILE_PROCFS;
      if (proc_pid != getpid()) {
        return;
      }
    }
  }
  Connection::doLocking();
  _checkpointed = false;
}

void dmtcp::FileConnection::updatePath()
{
  dmtcp::string link = "/proc/self/fd/" + jalib::XToString(_fds[0]);

  JTRACE("Update path from /proc fs:")(link);

  if (jalib::Filesystem::FileExists(link)) {
    _path = jalib::Filesystem::ResolveSymlink(link);
    JTRACE("Resolve symlink fs:")(link)(_path);
  }
}


void dmtcp::FileConnection::handleUnlinkedFile()
{
  if (!jalib::Filesystem::FileExists(_path) && !_isBlacklistedFile(_path)) {
    /* File not present in Filesystem.
     * /proc/self/fd lists filename of unlink()ed files as:
     *   "<original_file_name>(deleted)"
     */
    updatePath();

    if (Util::strEndsWith(_path, DELETED_FILE_SUFFIX)) {
      _path.erase(_path.length() - strlen(DELETED_FILE_SUFFIX));
      _type = FILE_DELETED;
    } else {
      JASSERT(_type == FILE_DELETED) (_path)
        .Text("File not found on disk and yet the filename doesn't "
              "contain the suffix '(deleted)'");
    }
  } else if (Util::strStartsWith(jalib::Filesystem::BaseName(_path), ".nfs")) {
    JWARNING(access(_path.c_str(), W_OK) == 0) (JASSERT_ERRNO);
    JTRACE(".nfsXXXX: files that are unlink()'d, "
           "but still in use by some process(es)")
      (_path);
    _type = FILE_DELETED;
  }
}

void dmtcp::FileConnection::calculateRelativePath()
{
  dmtcp::string cwd = jalib::Filesystem::GetCWD();
  if (_path.compare(0, cwd.length(), cwd) == 0) {
    /* CWD = "/A/B", FileName = "/A/B/C/D" ==> relPath = "C/D" */
    _rel_path = _path.substr(cwd.length() + 1);
  } else {
    _rel_path = "*";
  }
}

#if 0
void dmtcp::FileConnection::preCheckpointResMgrFile()
{
  JTRACE("Pre-checkpoint Torque files") (_fds.size());
  for (unsigned int i=0; i< _fds.size(); i++)
    JTRACE("_fds[i]=") (i) (_fds[i]);

  if (isTorqueIOFile(_path)) {
    _rmtype = TORQUE_IO;
    // Save the content of stdio or node file
    // to restore it later in new IO file or in temporal Torque nodefile
    saveFile(_fds[0]);
  } else if (isTorqueNodeFile(_path) || _rmtype == TORQUE_NODE) {
    _rmtype = TORQUE_NODE;
    // Save the content of stdio or node file
    // to restore it later in new IO file or in temporal Torque nodefile
    saveFile(_fds[0]);
  }
}
#endif

void dmtcp::FileConnection::preCheckpoint()
{
  JASSERT(_fds.size() > 0);

  handleUnlinkedFile();

  calculateRelativePath();

  _ckptFilesDir = UniquePid::getCkptFilesSubDir();

  // Read the current file descriptor offset
  _offset = lseek(_fds[0], 0, SEEK_CUR);
  fstat(_fds[0], &_stat);

  // If this file is related to supported Resource Management system
  // handle it specially
  if (_type == FILE_BATCH_QUEUE &&
      dmtcp_bq_should_ckpt_file &&
      dmtcp_bq_should_ckpt_file(_path.c_str(), &_rmtype)) {
      JTRACE("Pre-checkpoint Torque files") (_fds.size());
      for (unsigned int i=0; i< _fds.size(); i++)
        JTRACE("_fds[i]=") (i) (_fds[i]);
    saveFile(_fds[0]);
    return;
  }

  if (_isBlacklistedFile(_path)) {
    return;
  }
  if (getenv(ENV_VAR_CKPT_OPEN_FILES) != NULL &&
      _stat.st_uid == getuid()) {
    saveFile(_fds[0]);
  } else if (_type == FILE_DELETED) {
    saveFile(_fds[0]);
//   FIXME: Disable the following heuristic until we can comeup with a better
//   one
// } else if ((_fcntlFlags &(O_WRONLY|O_RDWR)) != 0 &&
//             _offset < _stat.st_size &&
//             _stat.st_size < MAX_FILESIZE_TO_AUTOCKPT &&
//             _stat.st_uid == getuid()) {
//    saveFile(_fds[0]);
  } else if (_isVimApp() &&
             (Util::strEndsWith(_path, ".swp") == 0 ||
              Util::strEndsWith(_path, ".swo") == 0)) {
    saveFile(_fds[0]);
  } else if (Util::strStartsWith(jalib::Filesystem::GetProgramName(),
                                 "emacs")) {
    saveFile(_fds[0]);
  } else {
  }
}

void dmtcp::FileConnection::refill(bool isRestart)
{
  struct stat buf;
  if (!_checkpointed) {
    JASSERT(jalib::Filesystem::FileExists(_path)) (_path)
      .Text("File not found.");

    if (stat(_path.c_str() ,&buf) == 0 && S_ISREG(buf.st_mode)) {
      if (buf.st_size > _stat.st_size &&
          (_fcntlFlags &(O_WRONLY|O_RDWR)) != 0) {
        errno = 0;
        JASSERT(truncate(_path.c_str(), _stat.st_size) ==  0)
          (_path.c_str()) (_stat.st_size) (JASSERT_ERRNO);
      } else if (buf.st_size < _stat.st_size) {
        JWARNING(false) .Text("Size of file smaller than what we expected");
      }
    }
    int tempfd = openFile();
    Util::dupFds(tempfd, _fds);
  }

  errno = 0;
  if (jalib::Filesystem::FileExists(_path) &&
      stat(_path.c_str() ,&buf) == 0 && S_ISREG(buf.st_mode)) {
    if (_offset <= buf.st_size && _offset <= _stat.st_size) {
      JASSERT(lseek(_fds[0], _offset, SEEK_SET) == _offset)
        (_path) (_offset) (JASSERT_ERRNO);
      //JTRACE("lseek(_fds[0], _offset, SEEK_SET)") (_fds[0]) (_offset);
    } else if (_offset > buf.st_size || _offset > _stat.st_size) {
      JWARNING(false) (_path) (_offset) (_stat.st_size) (buf.st_size)
        .Text("No lseek done:  offset is larger than min of old and new size.");
    }
  }
  refreshPath();
  restoreOptions();
}

void dmtcp::FileConnection::resume(bool isRestart)
{
  if (_checkpointed && isRestart && _type == FILE_DELETED) {
    /* Here we want to unlink the file. We want to do it only at the time of
     * restart, but there is no way of finding out if we are restarting or not.
     * That is why we look for the file on disk and if it is present(it was
     * deleted at ckpt time), then we assume that we are restarting and hence
     * we unlink the file.
     */
    if (jalib::Filesystem::FileExists(_path)) {
      JWARNING(unlink(_path.c_str()) != -1) (_path)
        .Text("The file was unlinked at the time of checkpoint. "
              "Unlinking it after restart failed");
    }
  }
}

void dmtcp::FileConnection::refreshPath()
{
  dmtcp::string cwd = jalib::Filesystem::GetCWD();

  if (_type == FILE_BATCH_QUEUE) {
    // get new file name
    dmtcp::string procpath = "/proc/self/fd/" + jalib::XToString(_fds[0]);
    dmtcp::string newpath = jalib::Filesystem::ResolveSymlink(procpath);
    JTRACE("This is Resource Manager file!") (_fds[0]) (newpath) (_path) (this);
    if (newpath != _path) {
      JTRACE("File Manager connection _path is changed => _path = newpath!")
        (_path) (newpath);
      _path = newpath;
    }
  } else if (_rel_path != "*" && !jalib::Filesystem::FileExists(_path)) {
    // If file at absolute path doesn't exist and file path is relative to
    // executable current dir
    string oldPath = _path;
    dmtcp::string fullPath = cwd + "/" + _rel_path;
    if (jalib::Filesystem::FileExists(fullPath)) {
      _path = fullPath;
      JTRACE("Change _path based on relative path")
        (oldPath) (_path) (_rel_path);
    }
  } else if (_type == FILE_PROCFS) {
    int index = 6;
    char *rest;
    char buf[64];
    pid_t proc_pid = strtol(&_path[index], &rest, 0);
    if (proc_pid > 0 && *rest == '/') {
      sprintf(buf, "/proc/%d/%s", getpid(), rest);
      _path = buf;
    }
  }
}

void dmtcp::FileConnection::postRestart()
{
  int tempfd;

  JASSERT(_fds.size() > 0);
  if (!_checkpointed) return;

  JTRACE("Restoring File Connection") (id()) (_path);
  dmtcp::string savedFilePath = getSavedFilePath(_path);
  JASSERT(jalib::Filesystem::FileExists(savedFilePath))
    (savedFilePath) (_path) .Text("Unable to Find checkpointed copy of File");

  if (_type == FILE_BATCH_QUEUE) {
    JASSERT(dmtcp_bq_restore_file);
    tempfd = dmtcp_bq_restore_file(_path.c_str(), savedFilePath.c_str(),
                               _fcntlFlags, _rmtype);
    JTRACE("Restore Resource Manager File") (_path);
  } else {
    refreshPath();
    JASSERT(jalib::Filesystem::FileExists(_path) == false) (_path)
      .Text("\n**** File already exists! Checkpointed copy can't be "
            "restored.\n"
            "****Delete the existing file and try again!");

    JNOTE("File not present, copying from saved checkpointed file") (_path);
    CreateDirectoryStructure(_path);
    JTRACE("Copying saved checkpointed file to original location")
      (savedFilePath) (_path);
    CopyFile(savedFilePath, _path);
    tempfd = openFile();
  }
  Util::dupFds(tempfd, _fds);
}

bool dmtcp::FileConnection::checkDup(int fd)
{
  bool retVal = false;

  int myfd = _fds[0];
  if ( lseek(myfd, 0, SEEK_CUR) == lseek(fd, 0, SEEK_CUR) ) {
    off_t newOffset = lseek (myfd, 1, SEEK_CUR);
    JASSERT (newOffset != -1) (JASSERT_ERRNO) .Text("lseek failed");

    if ( newOffset == lseek (fd, 0, SEEK_CUR) ) {
      retVal = true;
    }
    // Now restore the old offset
    JASSERT (-1 != lseek (myfd, -1, SEEK_CUR)) .Text("lseek failed");
  }
  return retVal;
}

static void CreateDirectoryStructure(const dmtcp::string& path)
{
  size_t index = path.rfind('/');

  if (index == dmtcp::string::npos)
    return;

  dmtcp::string dir = path.substr(0, index);

  index = path.find('/');
  while (index != dmtcp::string::npos) {
    if (index > 1) {
      dmtcp::string dirName = path.substr(0, index);

      int res = mkdir(dirName.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
      JASSERT(res != -1 || errno==EEXIST) (dirName) (path)
        .Text("Unable to create directory in File Path");
    }
    index = path.find('/', index+1);
  }
}

static void CopyFile(const dmtcp::string& src, const dmtcp::string& dest)
{
  dmtcp::string command = "cp -f " + src + " " + dest;
  JASSERT(_real_system(command.c_str()) != -1);
}

int dmtcp::FileConnection::openFile()
{
  JASSERT(jalib::Filesystem::FileExists(_path)) (_path)
    .Text("File not present");

  int fd = _real_open(_path.c_str(), _fcntlFlags);
  JASSERT(fd != -1) (_path) (JASSERT_ERRNO) .Text("open() failed");

  JTRACE("open(_path.c_str(), _fcntlFlags)") (fd) (_path.c_str()) (_fcntlFlags);
  return fd;
}

void dmtcp::FileConnection::saveFile(int fd)
{
  _checkpointed = true;

  dmtcp::string savedFilePath = getSavedFilePath(_path);
  CreateDirectoryStructure(savedFilePath);
  JTRACE("Saving checkpointed copy of the file") (_path) (savedFilePath);

  if (_type == FILE_REGULAR ||
      jalib::Filesystem::FileExists(_path)) {
    CopyFile(_path, savedFilePath);
    return;
  } else if (_type == FileConnection::FILE_DELETED) {
    long page_size = sysconf(_SC_PAGESIZE);
    const size_t bufSize = 2 * page_size;
    char *buf =(char*)JALLOC_HELPER_MALLOC(bufSize);

    int destFd = _real_open(savedFilePath.c_str(), O_CREAT | O_WRONLY | O_TRUNC,
                      S_IRWXU | S_IRWXG | S_IROTH |
                      S_IXOTH);
    JASSERT(destFd != -1) (_path) (savedFilePath) .Text("Read Failed");

    lseek(fd, 0, SEEK_SET);

    int readBytes, writtenBytes;
    while (1) {
      readBytes = Util::readAll(fd, buf, bufSize);
      JASSERT(readBytes != -1)
        (_path) (JASSERT_ERRNO) .Text("Read Failed");
      if (readBytes == 0) break;
      writtenBytes = Util::writeAll(destFd, buf, readBytes);
      JASSERT(writtenBytes != -1)
        (savedFilePath) (JASSERT_ERRNO) .Text("Write failed.");
    }

    close(destFd);
    JALLOC_HELPER_FREE(buf);
  }

  JASSERT(lseek(fd, _offset, SEEK_SET) != -1) (_path);
}

dmtcp::string dmtcp::FileConnection::getSavedFilePath(const dmtcp::string& path)
{
  dmtcp::ostringstream os;
  os << _ckptFilesDir
    << "/" << jalib::Filesystem::BaseName(_path) << "_" << _id.conId();

  return os.str();
}

void dmtcp::FileConnection::serializeSubClass(jalib::JBinarySerializer& o)
{
  JSERIALIZE_ASSERT_POINT("dmtcp::FileConnection");
  o & _path & _rel_path & _ckptFilesDir;
  o & _offset & _stat & _checkpointed & _rmtype;
  JTRACE("Serializing FileConn.") (_path) (_rel_path) (_ckptFilesDir)
   (_checkpointed) (_fcntlFlags);
}

/*****************************************************************************
 * FIFO Connection
 *****************************************************************************/

void dmtcp::FifoConnection::preCheckpoint()
{
  JASSERT(_fds.size() > 0);

  stat(_path.c_str(),&_stat);
  JTRACE("Checkpoint fifo.") (_fds[0]);

  int new_flags =(_fcntlFlags &(~(O_RDONLY|O_WRONLY))) | O_RDWR | O_NONBLOCK;
  ckptfd = _real_open(_path.c_str(),new_flags);
  JASSERT(ckptfd >= 0) (ckptfd) (JASSERT_ERRNO);

  _in_data.clear();
  size_t bufsize = 256;
  char buf[bufsize];
  int size;

  while (1) { // flush fifo
    size = read(ckptfd,buf,bufsize);
    if (size < 0) {
      break; // nothing to flush
    }
    for (int i=0;i<size;i++) {
      _in_data.push_back(buf[i]);
    }
  }
  close(ckptfd);
  JTRACE("Checkpointing fifo:  end.") (_fds[0]) (_in_data.size());
}

void dmtcp::FifoConnection::refill(bool isRestart)
{
  int new_flags =(_fcntlFlags &(~(O_RDONLY|O_WRONLY))) | O_RDWR | O_NONBLOCK;
  ckptfd = _real_open(_path.c_str(),new_flags);
  JASSERT(ckptfd >= 0) (ckptfd) (JASSERT_ERRNO);

  size_t bufsize = 256;
  char buf[bufsize];
  size_t j;
  ssize_t ret;
  for (size_t i=0;i<(_in_data.size()/bufsize);i++) { // refill fifo
    for (j=0; j<bufsize; j++) {
      buf[j] = _in_data[j+i*bufsize];
    }
    ret = Util::writeAll(ckptfd,buf,j);
    JASSERT(ret ==(ssize_t)j) (JASSERT_ERRNO) (ret) (j) (_fds[0]) (i);
  }
  int start =(_in_data.size()/bufsize)*bufsize;
  for (j=0; j<_in_data.size()%bufsize; j++) {
    buf[j] = _in_data[start+j];
  }
  errno=0;
  buf[j] ='\0';
  JTRACE("Buf internals.") ((const char*)buf);
  ret = Util::writeAll(ckptfd,buf,j);
  JASSERT(ret ==(ssize_t)j) (JASSERT_ERRNO) (ret) (j) (_fds[0]);

  close(ckptfd);
  // unlock fifo
  flock(_fds[0],LOCK_UN);
  JTRACE("End checkpointing fifo.") (_fds[0]);
  restoreOptions();
}

void dmtcp::FifoConnection::refreshPath()
{
  dmtcp::string cwd = jalib::Filesystem::GetCWD();
  if (_rel_path != "*") { // file path is relative to executable current dir
    string oldPath = _path;
    ostringstream fullPath;
    fullPath << cwd << "/" << _rel_path;
    if (jalib::Filesystem::FileExists(fullPath.str())) {
      _path = fullPath.str();
      JTRACE("Change _path based on relative path") (oldPath) (_path);
    }
  }
}

void dmtcp::FifoConnection::postRestart()
{
  JASSERT(_fds.size() > 0);
  JTRACE("Restoring Fifo Connection") (id()) (_path);
  refreshPath();
  int tempfd = openFile();
  Util::dupFds(tempfd, _fds);
  refreshPath();
}

int dmtcp::FifoConnection::openFile()
{
  int fd;

  if (!jalib::Filesystem::FileExists(_path)) {
    JTRACE("Fifo file not present, creating new one") (_path);
    mkfifo(_path.c_str(),_stat.st_mode);
  }

  fd = _real_open(_path.c_str(), O_RDWR | O_NONBLOCK);
  JTRACE("Is opened") (_path.c_str()) (fd);

  JASSERT(fd != -1) (_path) (JASSERT_ERRNO);
  return fd;
}

void dmtcp::FifoConnection::serializeSubClass(jalib::JBinarySerializer& o)
{
  JSERIALIZE_ASSERT_POINT("dmtcp::FifoConnection");
  o & _path & _rel_path & _savedRelativePath & _stat & _in_data;
  JTRACE("Serializing FifoConn.") (_path) (_rel_path) (_savedRelativePath);
}

/*****************************************************************************
 * Stdio Connection
 *****************************************************************************/

void dmtcp::StdioConnection::preCheckpoint()
{
  //JTRACE("Checkpointing stdio") (_fds[0]) (id());
}

void dmtcp::StdioConnection::refill(bool isRestart)
{
  restoreOptions();
}

void dmtcp::StdioConnection::postRestart()
{
  for (size_t i=0; i<_fds.size(); ++i) {
    int fd = _fds[i];
    if (fd <= 2) {
      JTRACE("Skipping restore of STDIO, just inherit from parent") (fd);
      continue;
    }
    int oldFd = -1;
    switch (_type) {
      case STDIO_IN:
        JTRACE("Restoring STDIN") (fd);
        oldFd=0;
        break;
      case STDIO_OUT:
        JTRACE("Restoring STDOUT") (fd);
        oldFd=1;
        break;
      case STDIO_ERR:
        JTRACE("Restoring STDERR") (fd);
        oldFd=2;
        break;
      default:
        JASSERT(false);
    }
    errno = 0;
    JWARNING(_real_dup2(oldFd, fd) == fd) (oldFd) (fd) (JASSERT_ERRNO);
  }
}

void dmtcp::StdioConnection::serializeSubClass(jalib::JBinarySerializer& o)
{
  JSERIALIZE_ASSERT_POINT("dmtcp::StdioConnection");
  //JTRACE("Serializing STDIO") (id());
}

/*****************************************************************************
 * POSIX Message Queue Connection
 *****************************************************************************/

void dmtcp::PosixMQConnection::on_mq_close()
{
}

void dmtcp::PosixMQConnection::on_mq_notify(const struct sigevent *sevp)
{
  if (sevp == NULL && _notifyReg) {
    _notifyReg = false;
  } else {
    _notifyReg = true;
    _sevp = *sevp;
  }
}

void dmtcp::PosixMQConnection::preCheckpoint()
{
  JASSERT(_fds.size() > 0);

  JTRACE("Checkpoint Posix Message Queue.") (_fds[0]);

  struct stat statbuf;
  JASSERT(fstat(_fds[0], &statbuf) != -1) (JASSERT_ERRNO);
  if (_mode == 0) {
    _mode = statbuf.st_mode;
  }

  struct mq_attr attr;
  JASSERT(mq_getattr(_fds[0], &attr) != -1) (JASSERT_ERRNO);
  _attr = attr;
  if (attr.mq_curmsgs < 0) {
    return;
  }

  int fd = _real_mq_open(_name.c_str(), O_RDWR, 0, NULL);
  JASSERT(fd != -1);

  _qnum = attr.mq_curmsgs;
  char *buf =(char*) JALLOC_HELPER_MALLOC(attr.mq_msgsize);
  for (long i = 0; i < _qnum; i++) {
    unsigned prio;
    ssize_t numBytes = _real_mq_receive(_fds[0], buf, attr.mq_msgsize, &prio);
    JASSERT(numBytes != -1) (JASSERT_ERRNO);
    _msgInQueue.push_back(jalib::JBuffer((const char*)buf, numBytes));
    _msgInQueuePrio.push_back(prio);
  }
  JALLOC_HELPER_FREE(buf);
  _real_mq_close(fd);
}

void dmtcp::PosixMQConnection::refill(bool isRestart)
{
  for (long i = 0; i < _qnum; i++) {
    JASSERT(_real_mq_send(_fds[0], _msgInQueue[i].buffer(),
                          _msgInQueue[i].size(), _msgInQueuePrio[i]) != -1);
  }
  _msgInQueue.clear();
  _msgInQueuePrio.clear();
}

void dmtcp::PosixMQConnection::postRestart()
{
  JASSERT(_fds.size() > 0);

  errno = 0;
  if (_oflag & O_EXCL) {
    mq_unlink(_name.c_str());
  }

  int tempfd = _real_mq_open(_name.c_str(), _oflag, _mode, &_attr);
  JASSERT(tempfd != -1) (JASSERT_ERRNO);
  Util::dupFds(tempfd, _fds);
}

void dmtcp::PosixMQConnection::serializeSubClass(jalib::JBinarySerializer& o)
{
  JSERIALIZE_ASSERT_POINT("dmtcp::PosixMQConnection");
  o & _name & _oflag & _mode & _attr;
}