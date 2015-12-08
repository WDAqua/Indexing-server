#Download part of DBpedia 2015
wget http://downloads.dbpedia.org/2015-04/core-i18n/en/instance-types_en.nt.bz2
wget http://downloads.dbpedia.org/2015-04/core-i18n/en/mappingbased-properties_en.nt.bz2
wget http://downloads.dbpedia.org/2015-04/core-i18n/en/labels_en.nt.bz2

#Decompress the downloaded data
bunzip instance-types_en.nt.bz2
bunzip mappingbased-properties_en.nt.bz2
bunzip labels_en.nt.bz2

#Make a dump
cat instance-types_en.nt mappingbased-properties_en.nt labels_en.nt > dump.nt
