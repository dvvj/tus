#/bin/bash
docker run -p 5601:5601 -p 9200:9200 -p 5044:5044 -it \
 -v "$PWD/logstash_pmid_aff_geo.conf":/etc/logstash/conf.d/30-output.conf \
 -v tv3:/app/esAffOut-b3 \
 -v tv2:/app/esAffOut-b2 \
 -v tv1:/app/esAffOut-b1 \
 -v "$PWD/pmid_aff_geo.json":/app/pmid_aff_geo.json \
 --name elk sebp/elk
