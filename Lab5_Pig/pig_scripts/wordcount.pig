lines = LOAD '/shared_volume/alice.txt';
words = FOREACH lines GENERATE FLATTEN(TOKENIZE((chararray)$0)) AS word;
clean_w = FILTER words BY word MATCHES '\\w+';
D = GROUP clean_w BY word;
E = FOREACH D GENERATE group AS word, COUNT(clean_w) AS cnt;
STORE E INTO '/shared_volume/pig_out/WORD_COUNT/';
