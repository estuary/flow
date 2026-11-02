#!/usr/bin/env python3
"""Dual-NIC routing lab. Run via ci:connector-vmm-kvm --routing-deployment, never on a plane.

The engine uses a private store and a host Podman API socket, in a private
network namespace. Its public and NAT NICs reach a router which simulates
cloud source NAT, then an endpoint which reports the source it observes.
The routing script is rendered from the deployment repository under test.
"""
import argparse
import concurrent.futures
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import socket
import subprocess
import tempfile
import time

import jinja2


def command(*args, input=None):
    result = subprocess.run([str(a) for a in args], input=input, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, timeout=120)
    if result.returncode:
        raise RuntimeError(f"{args}: {result.stderr}\n{result.stdout}")
    return result.stdout.strip()


GUEST = r'''
import json, socket, threading

def ingress():
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0',8080)); server.listen()
    while True:
        conn, _ = server.accept()
        with conn: conn.sendall(b'ingress\n')
threading.Thread(target=ingress, daemon=True).start()
server = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
server.bind((socket.VMADDR_CID_ANY,49092)); server.listen()
print(' ', flush=True)
while True:
    conn, _ = server.accept()
    with conn:
        for line in conn.makefile():
            args = json.loads(line)
            try:
                with socket.create_connection((args['host'],args.get('port',18443)),2) as s:
                    answer = s.recv(128).decode().strip()
            except OSError as error:
                answer = type(error).__name__
            conn.sendall((json.dumps(answer)+'\n').encode())
'''

ENDPOINT = r'''
import socket,struct,threading

def tcp():
    s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    s.bind(('0.0.0.0',18443)); s.listen()
    while True:
        c, peer=s.accept()
        with c: c.sendall((peer[0]+'\n').encode())
threading.Thread(target=tcp,daemon=True).start()
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.bind(('0.0.0.0',15353))
while True:
    data,peer=s.recvfrom(4096)
    end=12; labels=[]
    while data[end]:
        n=data[end]; labels.append(data[end+1:end+1+n].decode()); end+=n+1
    end+=5
    name='.'.join(labels)
    ip='10.20.0.2' if name=='private.acmeco.example' else '192.31.196.241'
    query=data[12:end]
    # The lab resolver only needs A. Refuse other types without authorizing them.
    qtype=struct.unpack('!H',data[end-4:end-2])[0]
    answer=(b'\xc0\x0c'+struct.pack('!HHIH',1,1,60,4)+socket.inet_aton(ip)) if qtype==1 else b''
    s.sendto(data[:2]+struct.pack('!HHHHH',0x8180,1,int(bool(answer)),0,0)+query+answer,peer)
'''



def store(root):
    return ['podman', '--root', str(root/'store'), '--runroot', str(root/'run'),
            '--tmpdir', str(root/'libpod'), '--network-config-dir', str(root/'networks'),
            '--storage-driver', 'overlay']


def recovery(root):
    """Recover only a recorded lab, after its Python owner has exited."""
    assert root.parent == Path('/tmp') and root.name.startswith('flow-vmm-route-'), root
    if not root.exists():
        return
    with (root/'owner').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest = json.loads((root/'manifest.json').read_text())
        for pid, started in reversed(manifest['processes']):
            def alive():
                try:
                    return Path(f'/proc/{pid}/stat').read_text().split(') ')[1].split()[19] == started
                except FileNotFoundError:
                    return False
            if alive():
                os.kill(pid, signal.SIGTERM)
                deadline = time.monotonic()+5
                while alive() and time.monotonic() < deadline:
                    time.sleep(.1)
                if alive():
                    os.kill(pid, signal.SIGKILL)
        namespaces = manifest['namespaces']
        host = f'fvr-{root.name[-6:]}-host'
        existing = {line.split()[0] for line in command('ip','netns','list').splitlines()}
        prefix = ['nsenter', f'--net=/run/netns/{host}'] if host in existing else []
        command(*prefix, *store(root), 'system', 'reset', '-f')
        for namespace in reversed(namespaces):
            assert namespace in [f'fvr-{root.name[-6:]}-{role}' for role in ['host','router','endpoint']]
            if namespace in existing:
                command('ip','netns','del',namespace)
    shutil.rmtree(root)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--deployment', required=True, type=Path)
    parser.add_argument('--resources', required=True, type=Path)
    parser.add_argument('--vmm-image', required=True)
    parser.add_argument('--connector-image', default='ghcr.io/estuary/derive-python:stable')
    parser.add_argument('--connector-init', required=True, type=Path)
    args = parser.parse_args()
    assert os.geteuid() == 0, 'run through the mise task (rootful isolated engine)'
    template = args.deployment / 'roles/common/templates/flow-vmm-routing.j2'
    routing = jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(
        template.read_text()).render(iface_nat_name='nat0')
    root = Path(tempfile.mkdtemp(prefix='flow-vmm-route-', dir='/tmp'))
    owner = (root/'owner').open('a')
    fcntl.flock(owner, fcntl.LOCK_EX)
    manifest = {'processes': [], 'namespaces': []}

    def save_manifest():
        staged = root/'manifest.tmp'
        staged.write_text(json.dumps(manifest))
        staged.replace(root/'manifest.json')

    save_manifest()
    with args.resources.open('a') as resources:
        resources.write(f'routing-lab {root}\n')
    suffix = root.name[-6:]
    namespaces = [f'fvr-{suffix}-{n}' for n in ('host', 'router', 'endpoint')]
    host, router, endpoint = namespaces
    processes = []
    created_namespaces = []
    service = None
    local = store(root)
    api = ['podman', '--remote', '--url', f'unix://{root}/api.sock']

    def ns(namespace, *argv, **kwargs):
        return command('nsenter', f'--net=/run/netns/{namespace}', *argv, **kwargs)

    def engine(*argv, **kwargs):
        return command(*api, *argv, **kwargs)

    def link(left, lname, laddr, right, rname, raddr):
        ns(left, 'ip', 'link', 'add', lname, 'type', 'veth', 'peer', 'name', rname,
           'netns', right)
        for namespace, name, address in [(left, lname, laddr), (right, rname, raddr)]:
            ns(namespace, 'ip', 'addr', 'add', address, 'dev', name)
            ns(namespace, 'ip', 'link', 'set', name, 'up')

    def spawn(namespace, argv, log):
        output = open(root/log, 'w')
        process = subprocess.Popen(['nsenter', f'--net=/run/netns/{namespace}', *argv],
                                   stdout=output, stderr=subprocess.STDOUT)
        output.close()
        processes.append(process)
        started = Path(f'/proc/{process.pid}/stat').read_text().split(') ')[1].split()[19]
        manifest['processes'].append([process.pid, started])
        save_manifest()
        return process

    def boundary(action, *argv):
        return engine('run', '--rm', '--network=host', '--read-only', '--cap-drop=all',
                      '--cap-add=NET_ADMIN', args.vmm_image, 'boundary', action, *argv)

    def launch(index, cidrs=()):
        name = f'vm{index}'
        bridge = f'fvm{index:012x}'
        state = root/name
        for directory in ['init', 'sock', 'scratch', 'mount']:
            (state/directory).mkdir(parents=True)
        (state/'init/policy.json').write_text(json.dumps({
            'egress': 'public', 'allowedNames': ['ok.acmeco.example', 'private.acmeco.example'],
            'declaredCidrs': list(cidrs)}))
        shutil.copyfile(args.connector_init, state/'mount/flow-connector-init')
        (state/'mount/flow-connector-init').chmod(0o555)
        (state/'mount/image-inspect.json').write_text(engine('image', 'inspect', args.connector_image))
        boundary('verify')
        engine('network', 'create', '--interface-name', bridge, name)
        engine('volume', 'create', '--driver=image', '--opt', f'image={args.connector_image}', name)
        owner = f'{index:032x}'
        boundary('grant', f'--owner={owner}', f'--bridge={bridge}', '--port=8080',
                 *[f'--cidr={cidr}' for cidr in cidrs])
        argv = ['run', '-d', '--name', name, '--network', name, '--read-only',
                '--read-only-tmpfs=false', '--device=/dev/kvm', '--device=/dev/net/tun',
                '--cap-add=NET_ADMIN', '--sysctl=net.ipv4.ip_forward=1',
                '--sysctl=net.ipv4.conf.default.rp_filter=1',
                '--sysctl=net.ipv6.conf.default.disable_ipv6=1', '--memory=768m', '--cpus=2',
                '--mount', f'type=volume,source={name},destination=/rootfs,readonly']
        for source, target in [('init','/init'), ('sock','/sock'), ('scratch','/scratch-backing'),
                               ('mount',str(state/'mount'))]:
            argv += ['--mount', f'type=bind,source={state/source},destination={target}' +
                     (',readonly' if source in ('init','mount') else '')]
        argv += ['--env', f'CONNECTOR_MOUNT={state}/mount', args.vmm_image, 'run',
                 '--policy=/init/policy.json', f'--connector-mount={state}/mount',
                 '--memory-mib=512', '--vcpus=2', '--disk-mib=256', '--ingress-port=8080',
                 '--resolver-upstream=192.31.196.241:15353', '--exec',
                 '/usr/local/bin/python', '-u', '-c', GUEST]
        engine(*argv)
        deadline = time.monotonic()+45
        while time.monotonic() < deadline:
            try:
                s = socket.socket(socket.AF_UNIX)
                s.connect(str(state/'sock/init.sock'))
                s.settimeout(5)
                s.sendall(b'{"host":"ok.acmeco.example"}\n')
                line = s.makefile().readline()
                if line:
                    s.close()
                    return name
            except OSError:
                pass
            time.sleep(.2)
        raise RuntimeError(engine('logs', name))

    def probe(name, address='ok.acmeco.example', port=18443):
        with socket.socket(socket.AF_UNIX) as s:
            s.settimeout(6)
            s.connect(str(root/name/'sock/init.sock'))
            s.sendall((json.dumps({'host':address, 'port':port})+'\n').encode())
            return json.loads(s.makefile().readline())

    def ordinary():
        code = "import socket; s=socket.create_connection(('192.31.196.241',18443),3); print(s.recv(128).decode().strip())"
        return engine('run', '--rm', '--network=ordinary', '--entrypoint=/usr/local/bin/python',
                      args.connector_image, '-c', code)

    try:
        # Archives make the lab's store independent of the host store's lifetime.
        for index, image in enumerate([args.vmm_image, args.connector_image]):
            archive = root/f'image{index}.tar'
            command('podman', 'save', '-o', archive, image)
            command(*local, 'load', '-i', archive)
            archive.unlink()
        for namespace in namespaces:
            assert not Path('/run/netns', namespace).exists(), namespace
            manifest['namespaces'].append(namespace)
            save_manifest()
            command('ip', 'netns', 'add', namespace)
            created_namespaces.append(namespace)
            ns(namespace, 'ip', 'link', 'set', 'lo', 'up')
            ns(namespace, 'sysctl', '-w', 'net.ipv4.ip_forward=1',
               'net.ipv4.conf.all.rp_filter=1', 'net.ipv4.conf.default.rp_filter=1')
        link(host, 'pub0', '10.0.0.2/24', router, 'pubgw', '10.0.0.1/24')
        link(host, 'nat0', '10.1.0.2/24', router, 'natgw', '10.1.0.1/24')
        link(router, 'edge0', '192.31.196.250/28', endpoint, 'edge1', '192.31.196.241/28')
        ns(router, 'ip', 'addr', 'add', '192.31.196.251/32', 'dev', 'edge0')
        ns(endpoint, 'ip', 'addr', 'add', '192.31.196.242/32', 'dev', 'edge1')
        ns(endpoint, 'ip', 'addr', 'add', '10.20.0.2/32', 'dev', 'lo')
        ns(endpoint, 'ip', 'route', 'add', 'default', 'via', '192.31.196.250')
        ns(router, 'ip', 'route', 'add', '10.20.0.2/32', 'via', '192.31.196.241')
        ns(host, 'ip', 'route', 'add', 'default', 'via', '10.0.0.1')
        ns(host, 'ip', 'route', 'add', '10.20.0.2/32', 'via', '10.0.0.1')
        ns(host, 'ip', 'route', 'add', 'default', 'via', '10.1.0.1', 'dev', 'nat0', 'table', '101')
        ns(host, 'ip', 'rule', 'add', 'priority', '32000', 'from', '10.22.0.0/24',
           'to', '10.22.0.0/24', 'table', 'main')
        ns(host, 'ip', 'rule', 'add', 'priority', '32000', 'from', '10.22.0.0/24',
           'to', '10.0.0.0/24', 'table', 'main')
        ns(host, 'ip', 'rule', 'add', 'priority', '32001', 'from', '10.22.0.0/24', 'table', '101')
        ns(router, 'nft', '-f', '-', input='''table ip cloud_nat {
chain postrouting { type nat hook postrouting priority srcnat; policy accept;
oifname "edge0" ip daddr 192.31.196.240/28 ip saddr 10.1.0.2 snat to 192.31.196.250
oifname "edge0" ip daddr 192.31.196.240/28 ip saddr 10.0.0.2 snat to 192.31.196.251
}
}
''')
        spawn(endpoint, ['python3', '-u', '-c', ENDPOINT], 'endpoint.log')
        service = spawn(host, [*local, 'system', 'service', '--time=0', f'unix://{root}/api.sock'], 'api.log')
        for _ in range(100):
            if (root/'api.sock').exists():
                break
            assert service.poll() is None, (root/'api.log').read_text()
            time.sleep(.1)
        engine('network', 'create', '--subnet=10.22.0.0/24', 'ordinary')
        boundary('install')
        vm = launch(1)
        before = {'ordinary': ordinary(), 'vmm': probe(vm)}
        print('before:', json.dumps(before), flush=True)
        assert before == {'ordinary':'192.31.196.250', 'vmm':'192.31.196.251'}, before
        ns(host, 'bash', '-s', input=routing)
        after = {'ordinary':ordinary(), 'vmm':probe(vm)}
        print('after:', json.dumps(after), flush=True)
        assert after == {'ordinary':'192.31.196.250', 'vmm':'192.31.196.250'}, after
        source_probe = "import socket; s=socket.create_connection(('192.31.196.241',18443),3); print(s.recv(128).decode().strip())"
        assert ns(host, 'python3', '-c', source_probe) == '192.31.196.251'
        engine('network', 'create', '--subnet=10.23.0.0/24', 'unrelated')
        assert engine('run', '--rm', '--network=unrelated', '--entrypoint=/usr/local/bin/python',
                      args.connector_image, '-c', source_probe) == '192.31.196.251'
        # Networkd labels its managed rules with a protocol. Reapplication
        # must recognize those rules too, rather than creating duplicates.
        for priority in ['31998', '31999']:
            ns(host, 'ip', 'rule', 'del', 'priority', priority)
        ns(host, 'ip', 'rule', 'add', 'priority', '31998', 'fwmark', '0x01000000/0x01000000',
           'table', 'main', 'suppress_prefixlength', '0', 'protocol', 'static')
        ns(host, 'ip', 'rule', 'add', 'priority', '31999', 'fwmark', '0x01000000/0x01000000',
           'table', '101', 'protocol', 'static')
        ns(host, 'bash', '-s', input=routing)
        rules = ns(host, 'ip', '-4', 'rule')
        assert rules.count('31998:') == rules.count('31999:') == 1
        # A new network, a new VMM and a private-plane CIDR grant, concurrently.
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            public_future = pool.submit(launch, 2)
            private_future = pool.submit(launch, 3, ['0.0.0.0/0'])
            public, private = public_future.result(), private_future.result()
        for name in [vm, public, private]:
            assert probe(name) == '192.31.196.250'
        assert probe(public, '192.31.196.242') == 'TimeoutError'
        assert probe(public, '10.20.0.2') == 'TimeoutError'
        assert probe(private, '10.20.0.2') == '10.0.0.2', 'private route must stay on pub0'
        assert probe(public, 'private.acmeco.example') != '10.0.0.2'
        ips = {name: json.loads(engine('inspect', name))[0]['NetworkSettings']['Networks'][name]['IPAddress']
               for name in [vm, public, private]}
        for name, address in ips.items():
            code = f"import socket; s=socket.create_connection(('{address}',8080),3); print(s.recv(128).decode().strip())"
            assert ns(host, 'python3', '-c', code) == 'ingress'
        assert probe(private, ips[public], 8080) == 'TimeoutError'
        boundary('revoke', f'--owner={3:032x}')
        assert probe(private, '10.20.0.2') == 'TimeoutError'
        engine('rm', '-f', vm)
        boundary('revoke', f'--owner={1:032x}')
        engine('network', 'rm', vm)
        engine('volume', 'rm', vm)
        assert probe(public) == '192.31.196.250'
        # Simulate loss/reinstallation of the route rules and nft table.
        ns(host, 'nft', 'delete', 'table', 'ip', 'flow_vmm_routing')
        ns(host, 'ip', 'rule', 'del', 'priority', '31998')
        ns(host, 'ip', 'rule', 'del', 'priority', '31999')
        ns(host, 'bash', '-s', input=routing)
        ns(host, 'bash', '-s', input=routing)
        recreated = launch(4, ['10.20.0.2/32'])
        assert probe(recreated, '10.20.0.2') == '10.0.0.2'
        assert probe(recreated) == probe(public) == '192.31.196.250'
        # Restart the API daemon while VMMs and their host conntrack remain.
        service.terminate()
        service.wait(timeout=10)
        service = spawn(host, [*local, 'system', 'service', '--time=0', f'unix://{root}/api.sock'], 'api-restarted.log')
        for _ in range(100):
            if (root/'api.sock').exists():
                try:
                    engine('info')
                    break
                except RuntimeError:
                    pass
            time.sleep(.1)
        ns(host, *local, 'network', 'reload', '--all')
        assert probe(recreated) == probe(public) == '192.31.196.250'
        rules = ns(host, 'ip', '-4', 'rule')
        assert rules.count('31998:') == rules.count('31999:') == 1
        print('PASS: DNS-only public policy, private 0/0 and internal route, ingress, VMM isolation, unrelated traffic, concurrent launches, teardown, routing recreation, API restart/network reload', flush=True)
        print('host rules:\n'+ns(host, 'ip', '-4', 'rule'), flush=True)
        print('host NAT rules:\n'+ns(host, 'nft', 'list', 'table', 'ip', 'nat'), flush=True)
        pid = str(json.loads(engine('inspect', public))[0]['State']['Pid'])
        print('VMM conntrack:\n'+command('nsenter', '-t', pid, '-n', 'conntrack', '-L', '-f', 'ipv4'), flush=True)
        print('router conntrack:\n'+ns(router, 'conntrack', '-L', '-f', 'ipv4'), flush=True)
        print('host conntrack:\n'+ns(host, 'conntrack', '-L', '-f', 'ipv4'), flush=True)
    finally:
        # Keep diagnostics before destroying our engine's containers and namespaces.
        try:
            if service is None:
                raise RuntimeError('API service was not started')
            for name in engine('ps', '-aq').splitlines():
                print(engine('logs', name), flush=True)
            engine('rm', '-af', '--time=0')
            engine('volume', 'rm', '-af')
        except RuntimeError as error:
            if service is not None:
                print(error, flush=True)
        for process in reversed(processes):
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        failed = []
        # Reset inside the engine's host network namespace: no cleanup command
        # may interpret an isolated bridge name in the machine's namespace.
        try:
            if host in created_namespaces:
                ns(host, *local, 'system', 'reset', '-f')
            else:
                command(*local, 'system', 'reset', '-f')
        except RuntimeError as error:
            failed.append(str(error))
        for namespace in reversed(created_namespaces) if not failed else []:
            try:
                command('ip', 'netns', 'del', namespace)
            except RuntimeError as error:
                failed.append(str(error))
        if failed:
            raise RuntimeError(f'Lab cleanup incomplete; retained {root}: {failed}')
        owner.close()
        shutil.rmtree(root)


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--cleanup':
        recovery(Path(sys.argv[2]))
    else:
        main()
