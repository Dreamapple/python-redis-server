import socketserver
import time

class Buffer(dict):
    def exist(self, key):
        if key not in self:
            return False
        v = self[key]
        if isinstance(v, BytesValue) and time.time()>v.out_time:
            del self[key]
            return False
        return True

buffer = Buffer()
class BytesValue:
    def __init__(self, value, expire=None):
        self.value = value
        self.out_time = time.time() + expire
        
        
class RedisTCPHandler(socketserver.StreamRequestHandler):
    allow_reuse_address = True
    request_queue_size = 20
    disable_nagle_algorithm = True

    def handle(self):
        """Handle multiple requests if necessary."""
        self.close_connection = 0

        self.handle_one_request()
        while not self.close_connection:
            self.handle_one_request()

    def handle_one_request(self):
        args = []
        n_arg = self.rfile.readline().strip()
        n_arg_ = int(n_arg[1:], 10)
        # assert n_arg[0] == ord('*'), n_arg
        for i in range(n_arg_):
            n_bytes = self.rfile.readline().strip()
            # assert n_bytes[0] == ord('$'), n_bytes
            # n_b = int(n_bytes[1:], 10)
            arg = self.rfile.readline().strip()
            # assert len(arg)==n_b, n_b
            args.append(arg)
            
        print("{} wrote:{}".format(self.client_address[0], repr(args)))
        resp = self.parse_request(args)
        self.wfile.write(self.parse_return(resp))
        #self.wfile.write(b'+OK\r\n')
        # self.wfile.flush()
    def parse_request(self, args):
        return getattr(self, args[0].decode(), self.default_handler)(*args)
    def SET(self, command, name, value, ex=0, px=0, nx=False, xx=False):
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` if it
            does not already exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` if it
            already exists.
        """
        if nx and name in buffer:
            return b'+OK\r\n'
        if xx and name not in buffer:
            return b'+OK\r\n'
        buffer[name] = BytesValue(value, ex+px*0.001)
        return True
    def GET(self, cmd, name):
        """
        Return the value at key ``name``, or None if the key doesn't exist
        """
        return buffer.get(name)
    def EXISTS(self, cmd, name):
        return 1 if name in buffer else 0
    def DEL(self, cmd, names):
        count = 0
        for name in names:
            if name in buffer:
                del buffer[name]
                count+=1
        return count
    def LLEN(self, cmd, name):
        l = buffer.get(name, [])
        assert isinstance(l, list)
        return len(l)
    def LPUSH(self, cmd, name, *values):
        if name not in buffer:
            buffer[name]=list()
        for v in values:
            buffer[name].append(v)
        return True
    def BRPOP(self, cmd, *keys):
        keys = list(keys)
        timeout = keys.pop()
        for k in keys:
            if k in buffer and buffer[k]:
                return (k, buffer[k].pop(0))
        return
    def KEYS(self, cmd, pattern):
        return ''
    def SETEX(self, cmd, k, ex, v):
        return self.SET(k, v, ex)
    def GETBIT(self, cmd, name, key):
        if name not in buffer:
            buffer[name] = {}
        return buffer[name].get(key,0)
    def SETBIT(self, cmd, name, k, v):
        old = self.GETBIT(cmd, name, k)
        buffer[name][k]=v
        return old
        
    def default_handler(self, *args):
        return input("你的回答：")
    def parse_return(self, ret):
        if isinstance(ret, BytesValue):
            return ('$%d\r\n%s\r\n'%(len(ret.value),ret.value.decode())).encode()
        if ret is True:
            return b'+OK\r\n'
        if ret is None:
            return b'$-1\r\n'
        if isinstance(ret, str):
            return ('$%d\r\n%s\r\n'%(len(ret),ret)).encode()
        if isinstance(ret, bytes):
            return ('$%d\r\n%s\r\n'%(len(ret),ret.decode())).encode()
        if isinstance(ret, int):
            return (':%d\r\n'%ret).encode()
        if isinstance(ret, (tuple,list)):
            return ('*%d\r\n'%len(ret)).encode()+b''.join(self.parse_return(k) for k in ret)

        return b'+OK\r\n'
        


if __name__ == "__main__":
    HOST, PORT = "localhost", 6379

    # Create the server, binding to localhost on port 6369
    server = socketserver.ThreadingTCPServer((HOST, PORT), RedisTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()
