#!/usr/bin/env python3
"""Exercise writes and fresh read fences with each of the three implementations
leading a cluster containing the other two. Build the Java probe and Rust/C++
binaries and run the Java tests first (their XML supplies the Java classpath).
Only child process groups started by this invocation are terminated.
"""
import os, pathlib, signal, socket, subprocess, time, xml.etree.ElementTree as ET

root = pathlib.Path(__file__).resolve().parents[1]
report = ET.parse(next((root/'raft-tests/target/surefire-reports').glob('TEST-*.xml')))
cp = next(p.attrib['value'] for p in report.findall('./properties/property') if p.attrib['name']=='java.class.path')
cp = str(root/'graft-cpp/java-probe/target/classes') + ':' + cp
cpp = os.environ.get('RAFT_CPP_BIN', str(root/'graft-cpp/build/graft_smoke'))
rust = os.environ.get('RAFT_RUST_BIN', str(root/'graft-rust/target/debug/graft-kv'))
import tempfile
directory = pathlib.Path(tempfile.mkdtemp(prefix='raft-consensus-smoke-'))

def run(args):
    result = subprocess.run(args, text=True, capture_output=True, timeout=12)
    if result.returncode:
        raise RuntimeError(f'{args[1:3]} failed: {result.stdout} {result.stderr}')
    return result.stdout

def ready(port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=.2): return True
    except OSError: return False

for number, leader in enumerate(['rust','cpp','java']):
    scenario = directory / f'{number}-{time.time_ns()}'
    scenario.mkdir()
    reservations = [socket.socket() for _ in range(3)]
    for reservation in reservations: reservation.bind(('127.0.0.1', 0))
    ports = dict(zip(['java','cpp','rust'], [s.getsockname()[1] for s in reservations]))
    for reservation in reservations: reservation.close()
    processes = []
    logs = []
    try:
        for language in [p for p in ports if p != leader] + [leader]:
            peers = [f'{p}@127.0.0.1:{port}' for p,port in ports.items() if p != language]
            port = str(ports[language])
            if language == 'java':
                args = ['java','-Djava.net.preferIPv4Stack=true','-cp',cp,'org.gautelis.raft.cpp.JavaPeerMain',language,'127.0.0.1',port,'2000' if language==leader else '30000',str(scenario/language),*peers]
            elif language == 'cpp':
                args = [cpp,'serve-active-persistent' if language==leader else 'serve-persistent','127.0.0.1',port,language,str(scenario/'cpp.state'),'0','0','0',*peers]
            else:
                args = [rust,'serve-active-persistent' if language==leader else 'serve-stateful','127.0.0.1',port,language]
                if language==leader: args += [str(scenario/'rust.state')]
                args += ['0','0','0',*peers]
            log = open(scenario/f'{language}.log','w')
            logs.append(log)
            processes.append(subprocess.Popen(args,stdout=log,stderr=subprocess.STDOUT,start_new_session=True))
            deadline = time.monotonic()+10
            while not ready(ports[language]) and time.monotonic()<deadline:
                if processes[-1].poll() is not None: raise RuntimeError(f'{language} exited; see {scenario}')
                time.sleep(.05)
            if not ready(ports[language]): raise RuntimeError(f'{language} never listened; see {scenario}')
        deadline = time.monotonic()+25
        while time.monotonic()<deadline:
            output = subprocess.run([cpp,'telemetry','127.0.0.1',str(ports[leader]),'true'], text=True, capture_output=True, timeout=12).stdout
            if 'state: LEADER' in output: break
            time.sleep(.2)
        else: raise RuntimeError(f'no {leader} leader; see {scenario}')
        put = run([cpp,'client-put','127.0.0.1',str(ports[leader]),'key','value','regression-client'])
        if 'success: true' not in put: raise RuntimeError(f'{leader} put failed: {put}; see {scenario}')
        for _ in range(2):
            get = run([cpp,'client-get','127.0.0.1',str(ports[leader]),'key','regression-client'])
            if 'success: true' not in get or 'value: value' not in get:
                raise RuntimeError(f'{leader} read failed: {get}; see {scenario}')
        print(f'{leader} leader with both other implementations: put and two fenced reads passed; logs {scenario}',flush=True)
    finally:
        for process in processes:
            if process.poll() is None: os.killpg(process.pid,signal.SIGTERM)
        for process in processes:
            try: process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid,signal.SIGKILL)
                process.wait()
        for log in logs: log.close()
