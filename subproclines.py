import select
import subprocess
from select import POLLIN, POLLHUP, EPOLLIN, EPOLLHUP
from fcntl import fcntl, F_GETFL, F_SETFL
from os import O_NONBLOCK, read

BUFSIZ = 1024 * 4

def xpoll_parallel_reader(streams, poll, POLLIN, POLLHUP, buffer_size=BUFSIZ):
	if buffer_size < 1:
		raise ValueError("buffer size must be >= 1")

	fds = []
	fd_map = {}

	for i, stream in enumerate(streams):
		fd = stream.fileno()

		fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK)

		poll.register(fd, POLLIN)
		fds.append(fd)
		fd_map[fd] = i

	data = [b''] * len(streams)
	while True:
		for i in range(len(data)):
			data[i] = b''

		got_data = False
		for fd, events in poll.poll():
			if events & POLLIN:
				chunk = data[fd_map[fd]] = read(fd, buffer_size)
				if chunk:
					got_data = True

		if not got_data:
			break

		yield tuple(data)

def epoll_parallel_reader(streams,buffer_size=BUFSIZ):
	try:
		poll = select.epoll()
		for item in xpoll_parallel_reader(streams, poll, EPOLLIN, EPOLLHUP, buffer_size):
			yield item
	finally:
		poll.close()

def poll_parallel_reader(streams,buffer_size=BUFSIZ):
	return xpoll_parallel_reader(streams, select.poll(), POLLIN, POLLHUP, buffer_size)

def select_parallel_reader(streams,buffer_size=BUFSIZ):
	rlist = []
	wlist = []
	xlist = []
	fd_map = {}

	for i, stream in enumerate(streams):
		fd = stream.fileno()

		fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK)
		rlist.append(fd)
		fd_map[fd] = i

	data = [b''] * len(streams)
	while True:
		for i in range(len(data)):
			data[i] = b''
		ravail, wavail, xavail = select.select(rlist, wlist, xlist)

		got_data = False
		for fd in ravail:
			chunk = data[fd_map[fd]] = read(fd, buffer_size)
			if chunk:
				got_data = True

		if not got_data:
			break

		yield tuple(data)

if hasattr(select,'epoll'):
	parallel_reader = epoll_parallel_reader
elif hasattr(select,'poll'):
	parallel_reader = poll_parallel_reader
else:
	parallel_reader = select_parallel_reader

def lines(parallel_streams):
	buffers = []
	lines = []
	for chunks in parallel_streams:
		n = len(chunks)
		d = n - len(buffers)
		while d > 0:
			buffers.append([])
			lines.append(b'')
			d -= 1

		got_line = False
		for i, chunk in enumerate(chunks):
			pos = chunk.find(b'\n')
			buf = buffers[i]
			if pos > -1:
				end = pos+1
				buf.append(chunk[:end])
				lines[i] = b''.join(buf)
				del buf[:]

				# XXX: broken for multiple '\n' in one chunk

				if end < len(chunk):
					buf.append(chunk[end:])

				got_line = True
			elif chunk:
				buf.append(chunk)
				lines[i] = b''
			elif buf:
				lines[i] = b''.join(buf)
				del buf[:]
				got_line = True
			else:
				lines[i] = b''

		if got_line:
			yield tuple(lines)

	if any(buffers):
		yield tuple(b''.join(buf) for buf in buffers)

def subprocchunks(args,buffer_size=BUFSIZ):
	proc = subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
	return parallel_reader([proc.stdout,proc.stderr],buffer_size)

def subproclines(args,buffer_size=BUFSIZ):
	proc = subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
	return lines(parallel_reader([proc.stdout,proc.stderr],buffer_size))
