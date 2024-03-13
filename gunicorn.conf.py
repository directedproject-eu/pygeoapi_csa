bind = '0.0.0.0:5000'
backlog = 2048

workers = 4
worker_class = 'sync'
worker_connections = 1000
timeout = 30
keepalive = 2


def post_fork(server, worker):
    server.log.info("Worker spawned (pid: %s)", worker.pid)


def pre_fork(server, worker):
    pass


def pre_exec(server):
    server.log.info("Forked child, re-executing.")


def when_ready(server):
    server.log.info("Server is ready. Spawning workers")
