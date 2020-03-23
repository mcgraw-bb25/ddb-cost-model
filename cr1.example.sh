./workloadRunner \
    -dbVendor="CockroachDB" \
    -isMultiServer=false \
    -isDistributedDB=true \
    -isMultiNode=false \
    -isMultiDC=false \
    -multiNodeCount=0 \
    -totalNetworkDist=0 \
    -connStr="postgresql://au418@111.111.111.111:26250/au418?sslmode=disable"
