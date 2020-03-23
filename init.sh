# various commands used to initialize clusters

# local single node

cockroach start-single-node \
--insecure \
--listen-addr=localhost:26250 \
--http-addr=localhost:9091 \
--cache=.25 \
--max-sql-memory=.25 \
--background

# login to servers
ssh root@111.111.111.111
ssh root@222.222.222.222
ssh root@333.333.333.333

# cockroachdb installation
sudo apt-get update
wget -qO- https://binaries.cockroachdb.com/cockroach-v19.2.4.linux-amd64.tgz | tar  xvz
cp -i cockroach-v19.2.4.linux-amd64/cockroach /usr/local/bin/

# networking config
sudo ufw allow ssh
sudo ufw allow 26257
sudo ufw enable


# local multi node
# 111.111.111.111
cockroach start \
--insecure \
--store=node1 \
--listen-addr=111.111.111.111:26257 \
--http-addr=111.111.111.111:8080 \
--join=111.111.111.111:26257,222.222.222.222:26257,333.333.333.333:26257 \
--cache=.25 \
--max-sql-memory=.25 \
--background

# 222.222.222.222
cockroach start \
--insecure \
--store=node2 \
--listen-addr=222.222.222.222:26257 \
--http-addr=222.222.222.222:8080 \
--join=111.111.111.111:26257,222.222.222.222:26257,333.333.333.333:26257 \
--cache=.25 \
--max-sql-memory=.25 \
--background

# 333.333.333.333
cockroach start \
--insecure \
--store=node3 \
--listen-addr=333.333.333.333:26257 \
--http-addr=333.333.333.333:8080 \
--join=111.111.111.111:26257,222.222.222.222:26257,333.333.333.333:26257 \
--cache=.25 \
--max-sql-memory=.25 \
--background

# start server
cockroach init --insecure --host=111.111.111.111:26257

# run migrations
cockroach sql --insecure --host=111.111.111.111:26257