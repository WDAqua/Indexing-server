# Indexing-server
How to use this libaray?

1- Download the dump of the ontology you want. For example download the dbpedia dumps: <br />
  wget http://downloads.dbpedia.org/2015-04/core-i18n/en/labels_en.nt.bz2 <br />
  wget http://downloads.dbpedia.org/2015-04/core-i18n/en/mappingbased-properties_en.nt.bz2 <br />
  wget http://downloads.dbpedia.org/2015-04/core-i18n/en/infobox-properties_en.nt.bz2 <br />
  Decompresse them ; ) <br />
  Make a unique dump using „cat mappingbased-properties_en.nt infobox-properties_en > dump-en.nt" <br />
2- Install octave <br />
3- Download the git repository: https://github.com/WDAqua/Indexing-server <br />
4- Go into src/main/java/Index.class and modify the two variables <br />
<br />
       String octavePath="/usr/bin/octave"; <br />
       String dump = "/ssd/dennis/dump-it.nt“; <br />
<br />
to the corresponding directories. For octave you can find it out with „which octave“ on CL. <br />
5- Do „mvn package“ to compile. (NOTE: you need java 8 due to dependencies to Jena) <br />
6- Now you can run the server with „java -jar „ on the corresponding jar in the target folder. <br />
<br />
The corresponing client can be found under: <br />
https://github.com/WDAqua/Indexing-client <br />

If you want to include in the distance computations the relations you can uncomment the corresponding part in the src/main/java/Index.class file and comment the privious part for computing the distance only for the instances. <br />
If you want to output the computations in a file you can uncomment the last comment in the src/main/java/Index.class file.
