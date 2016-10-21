#Download part of DBpedia 2016
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/instance_types_en.ttl.bz2
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/mappingbased_objects_en.ttl.bz2
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/infobox_properties_en.ttl.bz2

#Decompress the downloaded data
bunzip instance-types_en.ttl.bz2
bunzip mappingbased-properties_en.ttl.bz2
bunzip infobox_properties_en.ttl.bz2

#Make a dump
cat instance-types_en.ttl mappingbased-properties_en.ttl infobox_properties_en.ttl > dump.nt
