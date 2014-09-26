SET serveroutput ON;
CREATE TABLE QueryKeywordMapping (queryKeywordId number PRIMARY key, queryId number, queryWord varchar(400));

CREATE TABLE QueryAdvertiserMapping (queryId number, advertiserId number, totalBid float, cosineSimilarity float, qualityScore float, 
greedy_first_rank float, rounded_greedy_first number , greedy_second_rank float,  rounded_greedy_second number,
balance_first_rank float, rounded_balance_first number, balance_second_rank float, rounded_balance_second number,
generalized_first_rank float, rounded_generalized_first number, generalized_second_rank float, rounded_generalized_second number,
PRIMARY KEY (queryId, advertiserId), FOREIGN KEY(queryId) REFERENCES queries(queryId),  FOREIGN KEY(advertiserId) REFERENCES advertisers(advertiserId));

-- This table is for a query and an advertiser combination. Once their cosine similarity is populated, this table is flushed.
CREATE TABLE wordVectors(queryId number, advertiserId number, superSetWords varchar2(400), queryVector number, advertiserVector number, 
PRIMARY KEY (queryId, advertiserId, superSetWords), FOREIGN KEY(queryId) REFERENCES queries(queryId),  FOREIGN KEY(advertiserId) REFERENCES advertisers(advertiserId));

CREATE TABLE greedy_first_balance AS (SELECT advertiserid, budget AS balance FROM advertisers);
ALTER TABLE greedy_first_balance ADD CONSTRAINT pk_adver_id_1 PRIMARY KEY (advertiserid);
ALTER TABLE greedy_first_balance ADD(count_charge_times NUMBER  DEFAULT 0);
ALTER TABLE greedy_first_balance ADD CONSTRAINT fk_adver_id_1  FOREIGN KEY (advertiserid) REFERENCES advertisers (advertiserid);

CREATE TABLE greedy_second_balance AS (SELECT advertiserid, budget AS balance FROM advertisers);
ALTER TABLE greedy_second_balance ADD CONSTRAINT pk_adver_id_2 PRIMARY KEY (advertiserid);
ALTER TABLE greedy_second_balance ADD(count_charge_times NUMBER  DEFAULT 0);
ALTER TABLE greedy_second_balance ADD CONSTRAINT fk_adver_id_2  FOREIGN KEY (advertiserid) REFERENCES advertisers (advertiserid);

CREATE TABLE balance_algo_first_balance AS (SELECT advertiserid, budget AS balance FROM advertisers);
ALTER TABLE balance_algo_first_balance ADD CONSTRAINT pk_adver_id_3 PRIMARY KEY (advertiserid);
ALTER TABLE balance_algo_first_balance ADD(count_charge_times NUMBER  DEFAULT 0);
ALTER TABLE balance_algo_first_balance ADD CONSTRAINT fk_adver_id_3  FOREIGN KEY (advertiserid) REFERENCES advertisers (advertiserid);

CREATE TABLE balance_algo_second_balance AS (SELECT advertiserid, budget AS balance FROM advertisers);
ALTER TABLE balance_algo_second_balance ADD CONSTRAINT pk_adver_id_4 PRIMARY KEY (advertiserid);
ALTER TABLE balance_algo_second_balance ADD(count_charge_times NUMBER  DEFAULT 0);
ALTER TABLE balance_algo_second_balance ADD CONSTRAINT fk_adver_id_4  FOREIGN KEY (advertiserid) REFERENCES advertisers (advertiserid);

CREATE TABLE generalized_first_balance AS (SELECT advertiserid, budget AS balance FROM advertisers);
ALTER TABLE generalized_first_balance ADD CONSTRAINT pk_adver_id_5 PRIMARY KEY (advertiserid);
ALTER TABLE generalized_first_balance ADD(fraction_budget NUMBER DEFAULT 1);
ALTER TABLE generalized_first_balance ADD(count_charge_times NUMBER DEFAULT 0);
ALTER TABLE generalized_first_balance ADD CONSTRAINT fk_adver_id_5  FOREIGN KEY (advertiserid) REFERENCES advertisers (advertiserid);

CREATE TABLE generalized_second_balance AS (SELECT advertiserid, budget AS balance FROM advertisers);
ALTER TABLE generalized_second_balance ADD CONSTRAINT pk_adver_id_6 PRIMARY KEY (advertiserid);
ALTER TABLE generalized_second_balance ADD(fraction_budget NUMBER DEFAULT 1);
ALTER TABLE generalized_second_balance ADD(count_charge_times NUMBER  DEFAULT 0);
ALTER TABLE generalized_second_balance ADD CONSTRAINT fk_adver_id_6  FOREIGN KEY (advertiserid) REFERENCES advertisers (advertiserid);

CREATE TABLE greedy_first_output(queryid number, rank number, advertiserId number, balance float, budget float, 
PRIMARY KEY (queryid, advertiserid), 
FOREIGN KEY(queryid) REFERENCES queries(queryId), FOREIGN KEY(advertiserid) REFERENCES advertisers(advertiserid));

CREATE TABLE greedy_second_output(queryid number, rank number, advertiserId number, balance float, budget float, 
PRIMARY KEY (queryid, advertiserid), 
FOREIGN KEY(queryid) REFERENCES queries(queryId), FOREIGN KEY(advertiserid) REFERENCES advertisers(advertiserid));

CREATE TABLE balance_first_output(queryid number, rank number, advertiserId number, balance float, budget float, 
PRIMARY KEY (queryid, advertiserid), 
FOREIGN KEY(queryid) REFERENCES queries(queryId), FOREIGN KEY(advertiserid) REFERENCES advertisers(advertiserid));

CREATE TABLE balance_second_output(queryid number, rank number, advertiserId number, balance float, budget float, 
PRIMARY KEY (queryid, advertiserid), 
FOREIGN KEY(queryid) REFERENCES queries(queryId), FOREIGN KEY(advertiserid) REFERENCES advertisers(advertiserid));

CREATE TABLE generalized_first_output(queryid number, rank number, advertiserId number, balance float, budget float, 
PRIMARY KEY (queryid, advertiserid), 
FOREIGN KEY(queryid) REFERENCES queries(queryId), FOREIGN KEY(advertiserid) REFERENCES advertisers(advertiserid));

CREATE TABLE generalized_second_output(queryid number, rank number, advertiserId number, balance float, budget float, 
PRIMARY KEY (queryid, advertiserid), 
FOREIGN KEY(queryid) REFERENCES queries(queryId), FOREIGN KEY(advertiserid) REFERENCES advertisers(advertiserid));


CREATE SEQUENCE query_keyword_seq;

CREATE OR REPLACE TRIGGER query_keyword_trigger 
BEFORE INSERT ON querykeywordmapping 
FOR EACH ROW

BEGIN
  SELECT query_keyword_seq.NEXTVAL
  INTO   :new.QUERYKEYWORDID
  FROM   dual;
END;
/

CREATE TABLE valid_adver_greedy_second (queryid number, advertiserid number, 
PRIMARY KEY(queryid, advertiserid), FOREIGN KEY(queryid) REFERENCES queries(queryid), FOREIGN KEY(advertiserid) REFERENCES advertisers(advertiserid));

CREATE TABLE valid_adver_balance_second (queryid number, advertiserid number, 
PRIMARY KEY(queryid, advertiserid), FOREIGN KEY(queryid) REFERENCES queries(queryid), FOREIGN KEY(advertiserid) REFERENCES advertisers(advertiserid));

CREATE TABLE valid_adver_generalized_second (queryid number, advertiserid number, 
PRIMARY KEY(queryid, advertiserid), FOREIGN KEY(queryid) REFERENCES queries(queryid), FOREIGN KEY(advertiserid) REFERENCES advertisers(advertiserid));

DECLARE

PROCEDURE split_query_into_words_new(query_record IN queries%ROWTYPE) IS 
    
  no_of_appearance number;
  loop_counter number;
  previous_position number;
  current_position number;
  mydelimiter varchar(1) := ' ';
  query_word queries.queryText%TYPE;
  final_query_word queries.queryText%TYPE;
BEGIN
       
      -- Calculate the number of times space i.e. the delimiter has occured in the query text
      SELECT (LENGTH(query_record.queryText) - LENGTH(REPLACE(query_record.queryText,mydelimiter,'')))/LENGTH(mydelimiter) INTO no_of_appearance FROM dual;
      
      previous_position := 0;
      
      -- If number of appearance of delimiter is zero, then it means that query contains only one word
      IF no_of_appearance = 0 THEN
          -- Insert the entire word into the new table created.
          INSERT INTO QueryKeywordMapping (queryId, queryWord) VALUES (query_record.queryId, query_record.queryText);
          
      ELSE
          -- Run this loop no of times space i.e. delimiter occurs in the query text
          FOR loop_counter IN 1..no_of_appearance
          LOOP
            -- This gives the position of the delimiter for its nth appearance
            SELECT INSTR(query_record.queryText, mydelimiter, 1, loop_counter) INTO current_position FROM dual;
            
            -- This yields a sub string from previous appearance of the delimiter to the next appearance
            SELECT SUBSTR(query_record.queryText, previous_position, (current_position-previous_position)) INTO query_word FROM dual;
                  
            SELECT TRIM(both from query_word) INTO final_query_word FROM dual;
            
            -- Insert the separated query word into the new table created.
            INSERT INTO QueryKeywordMapping (queryId, queryWord) VALUES (query_record.queryId, query_word);
            previous_position := current_position + 1; 
            
          END LOOP;
          
          -- Select the last word
          current_position := LENGTH(query_record.queryText);
          SELECT SUBSTR(query_record.queryText, previous_position, (current_position-previous_position + 1)) INTO query_word FROM dual;
          
          -- Insert the separated query word into the new table created.
          INSERT INTO QueryKeywordMapping (queryId, queryWord) VALUES (query_record.queryId, query_word);
       
      END IF;
      
      COMMIT;

END split_query_into_words_new;

PROCEDURE find_valid_advertisers(query_id IN queries.queryId%TYPE) IS 

  -- Find advertisers who bid for at least one keyword in the query
    
BEGIN

    FOR adver in (SELECT DISTINCT q.queryid as qid ,k.advertiserid as aid FROM QueryKeywordMapping q, keywords k WHERE q.queryid = query_id AND (TRIM(both from LOWER(q.queryword)) LIKE TRIM(both from LOWER(k.keywords))))
    LOOP
    
      INSERT INTO QueryAdvertiserMapping(queryId, advertiserId) VALUES (adver.qid, adver.aid);

    END LOOP;
    COMMIT;
END find_valid_advertisers;


-- This procedure generates vectors which are used while calculating cosine similarity. It also calculates total bid and adds the value in the table.
PROCEDURE generate_vectors (query_id IN queries.queryId%TYPE, advertiser_id IN advertisers.advertiserid%TYPE) IS 

      word_presence_count number := 0;
      if_advertiser_bids number := 0;
      temp_count number;
      adv_keyword_rec keywords%ROWTYPE;
      total_bid keywords.bid%TYPE := 0;
      temp_bid keywords.bid%TYPE;
      
BEGIN
      -- This loop ensures to build vector for query and advertiser for every token in the query
      FOR fetched_word IN (SELECT * FROM querykeywordmapping WHERE queryid = query_id)
      LOOP
          temp_count := 0;
          SELECT count(queryVector) INTO temp_count FROM wordVectors WHERE queryid = query_id AND advertiserid = advertiser_id AND (TRIM(both from LOWER(superSetWords)) LIKE TRIM(both from LOWER(fetched_word.queryword)));
          
          IF(temp_count > 0) THEN
              
              -- If the count is > 0, then the word is already present in the query and it is encountered one more time. So just update the count.
              SELECT queryVector INTO word_presence_count FROM wordVectors WHERE queryid = query_id AND advertiserid = advertiser_id AND (TRIM(both from LOWER(superSetWords)) LIKE TRIM(both from LOWER(fetched_word.queryword)));
              word_presence_count := word_presence_count + 1;
              UPDATE wordVectors SET queryVector = word_presence_count WHERE queryid = query_id AND advertiserid = advertiser_id AND (TRIM(both from LOWER(superSetWords)) LIKE TRIM(both from LOWER(fetched_word.queryword)));
              
          ELSE
            
            word_presence_count := 1;
            -- This word is encountered for the first time. So whether advertisers bids for it or not is NOT known.         
            SELECT count(*) INTO if_advertiser_bids FROM keywords WHERE advertiserId = advertiser_id and (TRIM(both from LOWER(keywords)) LIKE TRIM(both from LOWER(fetched_word.queryword)));
            INSERT INTO wordVectors(queryid, advertiserid, superSetWords, queryVector, advertiserVector) VALUES (query_id, advertiser_id, fetched_word.queryword, word_presence_count, if_advertiser_bids);
            
            -- If the advertiser is bidding for that keyword, then we need to add it to the total bid
            IF(if_advertiser_bids > 0) THEN
            
                SELECT bid INTO temp_bid FROM keywords WHERE advertiserId = advertiser_id and (TRIM(both from LOWER(keywords)) LIKE TRIM(both from LOWER(fetched_word.queryword)));
                total_bid := total_bid + temp_bid;
            
            END IF;
            
          END IF;
          
      END LOOP;
      
      --After this loop ends, we need to insert total bid for this query-advertiser pair into the table
      UPDATE QueryAdvertiserMapping SET totalBid = total_bid WHERE queryId = query_id AND advertiserId = advertiser_id;
      
      -- This loop ensures that every keyword advertiser bids for is taken into consideration while building vectors and if it is not, then adds it into the vector
      -- But these keywords need not be considered for bidding since they are not a part of the query
      FOR adv_keyword_rec in (SELECT * FROM keywords WHERE advertiserid = advertiser_id)
      LOOP
          temp_count := 0;
          SELECT count(superSetWords) INTO temp_count FROM wordVectors WHERE queryid = query_id AND advertiserid = advertiser_id AND(TRIM(both from LOWER(superSetWords)) LIKE TRIM(both from LOWER(adv_keyword_rec.keywords)));
          
          IF(temp_count = 0) THEN
              -- If the count is 0 then it means that this word is not present in the query and is not considered in vector.
              INSERT INTO wordVectors (queryid, advertiserid, superSetWords, queryVector, advertiserVector) VALUES (query_id, advertiser_id, adv_keyword_rec.keywords, 0, 1);
                      
          END IF;      
      
      END LOOP;
      
      COMMIT;
      
END generate_vectors;



PROCEDURE calc_cosine_and_qualityscore (query_id IN queries.queryId%TYPE, advertiser_id IN advertisers.advertiserid%TYPE) IS

  total_product number := 0;
  temp_product number;
  total_query_square number := 0;
  temp_query_square number;
  total_adver_square number :=0;
  temp_adver_square number;
  adv_keyword_rec wordVectors%ROWTYPE;
  cosine queryadvertisermapping.cosinesimilarity%TYPE := 0;
  total_query_sqrt float := 0;
  total_adver_sqrt float := 0;
  quality_sc queryadvertisermapping.qualityscore%TYPE := 0;
  ctc_var advertisers.ctc%TYPE;
  temp_sqrt_product float := 0 ;
  

BEGIN
  
  FOR adv_keyword_rec in (SELECT (queryVector * advertiserVector) as temp_product , POWER(queryVector, 2)  as temp_query_square, POWER(advertiserVector, 2) as temp_adver_square 
  FROM wordVectors WHERE queryId = query_id AND advertiserId = advertiser_id)
  LOOP
  
  total_product := total_product + adv_keyword_rec.temp_product;
  total_query_square := total_query_square + adv_keyword_rec.temp_query_square;
  total_adver_square := total_adver_square + adv_keyword_rec.temp_adver_square;
 
  END LOOP;
  
    --After this loop ends, we need to calculate cosine similarity for this query-advertiser pair
    SELECT SQRT(total_query_square) INTO total_query_sqrt FROM DUAL;
    SELECT SQRT(total_adver_square) INTO total_adver_sqrt FROM DUAL;
    temp_sqrt_product := (total_query_sqrt * total_adver_sqrt);
 
    IF(temp_sqrt_product != 0) THEN
      cosine := (total_product) / temp_sqrt_product;
    END IF;
  
    SELECT ctc INTO ctc_var FROM advertisers WHERE advertiserId = advertiser_id;

    quality_sc := (cosine * ctc_var);
  
    UPDATE QueryAdvertiserMapping SET cosineSimilarity = cosine, qualityscore = quality_sc WHERE queryId = query_id AND advertiserId = advertiser_id;
     
    COMMIT;
  
END calc_cosine_and_qualityscore;

PROCEDURE calculate_basics(query_id IN queries.queryId%TYPE) IS

  advertiser_rec advertisers%ROWTYPE;

BEGIN

    FOR advertiser_rec IN (SELECT * FROM QueryAdvertiserMapping WHERE queryid = query_id)
    LOOP

        --Create the vectors for query, advertiser pair
        generate_vectors(query_id, advertiser_rec.advertiserid);

        calc_cosine_and_qualityscore(query_id, advertiser_rec.advertiserid);
        
    END LOOP;

END calculate_basics;

PROCEDURE greedy_first_op_write(query_id IN queries.queryId%TYPE) IS
  
  query_adver_rec QueryAdvertiserMapping%ROWTYPE;
  current_balance greedy_first_balance.balance%TYPE;
  current_budget advertisers.budget%TYPE;
  cur_task_limit systemInParams.TASKLIMIT%TYPE;
  
BEGIN
  
  SELECT TASKLIMIT INTO cur_task_limit FROM systemInParams WHERE lower(taskname) like lower('%TASK1%');
  
  FOR query_adver_rec IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id  AND GREEDY_FIRST_RANK!= 0 AND rounded_greedy_first <= cur_task_limit ORDER BY rounded_greedy_first ASC)
   LOOP
      
      SELECT budget INTO current_budget FROM advertisers WHERE advertiserId = query_adver_rec.advertiserId;
      SELECT balance INTO current_balance FROM greedy_first_balance WHERE advertiserid = query_adver_rec.advertiserid;
   
      INSERT INTO greedy_first_output (queryid, rank, advertiserId, balance, budget) VALUES (query_id, query_adver_rec.rounded_greedy_first, query_adver_rec.advertiserId, current_balance, current_budget);
   
   END LOOP;
   COMMIT;
END greedy_first_op_write;


-- This will calculate advertisers for greedy first price algorithm
PROCEDURE greedy_first_price(query_id IN queries.queryId%TYPE) IS

  query_adver_rec QueryAdvertiserMapping%ROWTYPE;
  current_balance greedy_first_balance.balance%TYPE;
  count_charge greedy_first_balance.count_charge_times%TYPE;
  ad_rank QueryAdvertiserMapping.greedy_first_rank%TYPE;
  hundred_ctc_val advertisers.hundred_ctc%TYPE;
  mod_value number;
  query_adv_rec QueryAdvertiserMapping%ROWTYPE;
  cur_task_limit systemInParams.TASKLIMIT%TYPE;
   
BEGIN

    FOR query_adver_rec IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id)
    LOOP
       
      SELECT balance INTO current_balance FROM greedy_first_balance WHERE advertiserid = query_adver_rec.advertiserid;
      --Check if the balance is sufficient to pay for bid
      IF (current_balance >= query_adver_rec.totalBid ) THEN
        --Calculate ad rank on the basis of bid
        ad_rank := (query_adver_rec.totalBid * query_adver_rec.qualityScore);
  
      ELSE
        -- Put ad rank as 0
        ad_rank := 0;
      
      END IF;

      UPDATE QueryAdvertiserMapping SET GREEDY_FIRST_RANK = ad_rank WHERE queryId = query_id AND advertiserId = query_adver_rec.advertiserId;

    END LOOP;    
      
      current_balance := 0;
      --Now get the actual rank from calculated rank
       FOR rank_rec IN (SELECT advertiserId, RANK() OVER (PARTITION BY queryid ORDER BY greedy_first_rank DESC, advertiserId ASC) as actual_rank FROM QueryAdvertiserMapping WHERE queryid = query_id) 
       LOOP 
          UPDATE QueryAdvertiserMapping SET rounded_greedy_first = rank_rec.actual_rank WHERE queryid = query_id AND advertiserId = rank_rec.advertiserId;
       END LOOP;
    
      --Charge only those advertisers who will be within specified ranks
      SELECT TASKLIMIT INTO cur_task_limit FROM systemInParams WHERE TRIM(both from lower(taskname)) like TRIM(both from lower('%TASK1%'));
    
      FOR tmpRecord IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id AND GREEDY_FIRST_RANK!= 0 AND rounded_greedy_first <= cur_task_limit ORDER BY rounded_greedy_first ASC)
      LOOP
            SELECT hundred_ctc INTO hundred_ctc_val FROM advertisers WHERE advertiserid = tmpRecord.advertiserid;
            SELECT balance, count_charge_times INTO current_balance, count_charge FROM greedy_first_balance WHERE advertiserid = tmpRecord.advertiserid;
        
            -- Now decide whether to charge this advertiser or not and if we need to charge then what is the amount
            SELECT MOD(count_charge,100) INTO mod_value FROM dual;
     
            IF( mod_value < hundred_ctc_val) THEN
              -- Charge the advertiser
              current_balance := current_balance - tmpRecord.totalBid;
              UPDATE greedy_first_balance SET balance = current_balance, count_charge_times = (count_charge + 1) WHERE advertiserId = tmpRecord.advertiserId;
            ELSE
              -- Do not charge but increase the advertiser charge count
               UPDATE greedy_first_balance SET count_charge_times = (count_charge + 1) WHERE advertiserId = tmpRecord.advertiserId;        
            END IF;   

      END LOOP;
      
     COMMIT;
   
     BEGIN
        greedy_first_op_write(query_id);
     END;
   
END greedy_first_price;

PROCEDURE greedy_second_op_write(query_id IN queries.queryId%TYPE) IS
  
  query_adver_rec QueryAdvertiserMapping%ROWTYPE;
  current_balance greedy_second_balance.balance%TYPE;
  current_budget advertisers.budget%TYPE;
  cur_task_limit systemInParams.TASKLIMIT%TYPE;
  
BEGIN
  
  SELECT TASKLIMIT INTO cur_task_limit FROM systemInParams WHERE lower(taskname) like lower('%TASK2%');
  
  FOR query_adver_rec IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id  AND GREEDY_SECOND_RANK!= 0 AND ROUNDED_GREEDY_SECOND <= cur_task_limit ORDER BY ROUNDED_GREEDY_SECOND ASC)
   LOOP
      
      SELECT budget INTO current_budget FROM advertisers WHERE advertiserId = query_adver_rec.advertiserId;
      SELECT balance INTO current_balance FROM greedy_second_balance WHERE advertiserid = query_adver_rec.advertiserid;
   
      INSERT INTO greedy_second_output (queryid, rank, advertiserId, balance, budget) VALUES (query_id, query_adver_rec.ROUNDED_GREEDY_SECOND, query_adver_rec.advertiserId, current_balance, current_budget);
   
   END LOOP;
   COMMIT;
END greedy_second_op_write;


-- This will calculate advertisers for greedy second price algorithm
PROCEDURE greedy_second_price(query_id IN queries.queryId%TYPE) IS

  query_adver_rec QueryAdvertiserMapping%ROWTYPE;
  current_balance greedy_second_balance.balance%TYPE;
  count_charge greedy_second_balance.count_charge_times%TYPE;
  ad_rank QueryAdvertiserMapping.greedy_second_rank%TYPE;
  hundred_ctc_val advertisers.hundred_ctc%TYPE;
  mod_value number;
  query_adv_rec QueryAdvertiserMapping%ROWTYPE;
  cur_task_limit systemInParams.TASKLIMIT%TYPE;
  bid_val queryadvertisermapping.totalbid%TYPE;
  tmp_count number := 0;
  
BEGIN

    --Find valid advertisers for this particular query on the basis of their current balance.
    FOR valid_adver_rec IN (SELECT queryId, q.advertiserid AS advertiserId FROM QueryAdvertiserMapping q, greedy_second_balance g WHERE  q.queryId = query_id AND q.advertiserid = g.advertiserid AND g.balance >= q.totalbid)    
    LOOP
    
      INSERT INTO valid_adver_greedy_second (queryId, advertiserId) VALUES (valid_adver_rec.queryId, valid_adver_rec.advertiserId);
    
    END LOOP;
     

    FOR query_adver_rec IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id)
    LOOP
       
      SELECT balance INTO current_balance FROM greedy_second_balance WHERE advertiserid = query_adver_rec.advertiserid;
      --Check if the balance is sufficient to pay for bid
      IF (current_balance >= query_adver_rec.totalBid ) THEN
        --Calculate ad rank on the basis of bid
        ad_rank := (query_adver_rec.totalBid * query_adver_rec.qualityScore);
  
      ELSE
        -- Put ad rank as 0
        ad_rank := 0;
      
      END IF;

      UPDATE QueryAdvertiserMapping SET GREEDY_SECOND_RANK = ad_rank WHERE queryId = query_id AND advertiserId = query_adver_rec.advertiserId;

    END LOOP;    
      
      current_balance := 0;
      --Now get the actual rank from calculated rank
       FOR rank_rec IN (SELECT advertiserId, RANK() OVER (PARTITION BY queryid ORDER BY GREEDY_SECOND_RANK DESC, advertiserId ASC) as actual_rank FROM QueryAdvertiserMapping WHERE queryid = query_id) 
       LOOP 
          UPDATE QueryAdvertiserMapping SET ROUNDED_GREEDY_SECOND = rank_rec.actual_rank WHERE queryid = query_id AND advertiserId = rank_rec.advertiserId;
       END LOOP;
    
      --Charge only those advertisers who will be within specified ranks
      SELECT TASKLIMIT INTO cur_task_limit FROM systemInParams WHERE TRIM(both from lower(taskname)) like TRIM(both from lower('%TASK2%'));
    
      FOR tmpRecord IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id AND GREEDY_SECOND_RANK!= 0 AND ROUNDED_GREEDY_SECOND <= cur_task_limit ORDER BY ROUNDED_GREEDY_SECOND ASC)
      LOOP
            SELECT hundred_ctc INTO hundred_ctc_val FROM advertisers WHERE advertiserid = tmpRecord.advertiserid;
            SELECT balance, count_charge_times INTO current_balance, count_charge FROM greedy_second_balance WHERE advertiserid = tmpRecord.advertiserid;
            
            tmp_count := 0;
            
            SELECT count(q.totalBid) INTO tmp_count FROM queryadvertisermapping q, greedy_second_balance g 
            WHERE q.queryId = query_id AND q.totalBid < tmpRecord.totalBid AND q.advertiserid IN (SELECT t.advertiserId FROM valid_adver_greedy_second t WHERE t.queryId = query_id)  
            ORDER BY q.totalBid DESC;
            
            IF (tmp_count > 0) THEN
                -- If there is such bid, then pay by that bid
                SELECT otherbid INTO bid_val FROM(SELECT q.totalBid as otherbid FROM queryadvertisermapping q, greedy_second_balance g 
                WHERE q.queryId = query_id AND q.totalBid < tmpRecord.totalBid AND q.advertiserid IN (SELECT t.advertiserId FROM valid_adver_greedy_second t WHERE t.queryId = query_id)  
                ORDER BY q.totalBid DESC) WHERE ROWNUM = 1 ;
            
            ELSE
              -- If there is no such bid, then pay by his/her own bid
              bid_val := tmpRecord.totalBid;
            
            END IF;
        
            -- Now decide whether to charge this advertiser or not and if we need to charge then what is the amount
            SELECT MOD(count_charge,100) INTO mod_value FROM dual;
     
            IF( mod_value < hundred_ctc_val) THEN
              -- Charge the advertiser
              current_balance := current_balance - bid_val;
              UPDATE greedy_second_balance SET balance = current_balance, count_charge_times = (count_charge + 1) WHERE advertiserId = tmpRecord.advertiserId;
            ELSE
              -- Do not charge but increase the advertiser charge count
               UPDATE greedy_second_balance SET count_charge_times = (count_charge + 1) WHERE advertiserId = tmpRecord.advertiserId;        
            END IF;   

      END LOOP;
      
     COMMIT;
   
     BEGIN
        greedy_second_op_write(query_id);
     END;
   
END greedy_second_price;


PROCEDURE balance_first_op_write(query_id IN queries.queryId%TYPE) IS
  
  query_adver_rec QueryAdvertiserMapping%ROWTYPE;
  current_balance balance_algo_first_balance.balance%TYPE;
  current_budget advertisers.budget%TYPE;
  cur_task_limit systemInParams.TASKLIMIT%TYPE;
  
BEGIN
  
  SELECT TASKLIMIT INTO cur_task_limit FROM systemInParams WHERE lower(taskname) like lower('%TASK3%');
  
  FOR query_adver_rec IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id  AND BALANCE_FIRST_RANK!= 0 AND rounded_balance_first <= cur_task_limit ORDER BY rounded_balance_first ASC)
   LOOP
      
      SELECT budget INTO current_budget FROM advertisers WHERE advertiserId = query_adver_rec.advertiserId;
      SELECT balance INTO current_balance FROM balance_algo_first_balance WHERE advertiserid = query_adver_rec.advertiserid;
   
      INSERT INTO balance_first_output (queryid, rank, advertiserId, balance, budget) VALUES (query_id, query_adver_rec.rounded_balance_first, query_adver_rec.advertiserId, current_balance, current_budget);
   
   END LOOP;
   COMMIT;
END balance_first_op_write;

-- This will calculate advertisers for balance first price algorithm
PROCEDURE balance_first_price(query_id IN queries.queryId%TYPE) IS

  query_adver_rec QueryAdvertiserMapping%ROWTYPE;
  current_balance balance_algo_first_balance.balance%TYPE;
  count_charge balance_algo_first_balance.count_charge_times%TYPE;
  ad_rank QueryAdvertiserMapping.balance_first_rank%TYPE;
  hundred_ctc_val advertisers.hundred_ctc%TYPE;
  mod_value number;
  query_adv_rec QueryAdvertiserMapping%ROWTYPE;
  cur_task_limit systemInParams.TASKLIMIT%TYPE;
   
BEGIN

    FOR query_adver_rec IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id)
    LOOP
       
      SELECT balance INTO current_balance FROM balance_algo_first_balance WHERE advertiserid = query_adver_rec.advertiserid;
      --Check if the balance is sufficient to pay for bid
      IF (current_balance >= query_adver_rec.totalBid ) THEN
        --Calculate ad rank on the basis of balance
        ad_rank := (current_balance * query_adver_rec.qualityScore);
  
      ELSE
        -- Put ad rank as 0
        ad_rank := 0;
      
      END IF;

      UPDATE QueryAdvertiserMapping SET BALANCE_FIRST_RANK = ad_rank WHERE queryId = query_id AND advertiserId = query_adver_rec.advertiserId;

    END LOOP;    
      
      current_balance := 0;
      --Now get the actual rank from calculated rank
       FOR rank_rec IN (SELECT advertiserId, RANK() OVER (PARTITION BY queryid ORDER BY BALANCE_FIRST_RANK DESC, advertiserId ASC) as actual_rank FROM QueryAdvertiserMapping WHERE queryid = query_id) 
       LOOP 
          UPDATE QueryAdvertiserMapping SET rounded_balance_first = rank_rec.actual_rank WHERE queryid = query_id AND advertiserId = rank_rec.advertiserId;
       END LOOP;
    
      --Charge only those advertisers who will be within specified ranks
      SELECT TASKLIMIT INTO cur_task_limit FROM systemInParams WHERE TRIM(both from lower(taskname)) like TRIM(both from lower('%TASK3%'));
    
      FOR tmpRecord IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id AND BALANCE_FIRST_RANK!= 0 AND rounded_balance_first <= cur_task_limit ORDER BY rounded_balance_first ASC)
      LOOP
            SELECT hundred_ctc INTO hundred_ctc_val FROM advertisers WHERE advertiserid = tmpRecord.advertiserid;
            SELECT balance, count_charge_times INTO current_balance, count_charge FROM balance_algo_first_balance WHERE advertiserid = tmpRecord.advertiserid;
        
            -- Now decide whether to charge this advertiser or not and if we need to charge then what is the amount
            SELECT MOD(count_charge,100) INTO mod_value FROM dual;
     
            IF( mod_value < hundred_ctc_val) THEN
              -- Charge the advertiser
              current_balance := current_balance - tmpRecord.totalBid;
              UPDATE balance_algo_first_balance SET balance = current_balance, count_charge_times = (count_charge + 1) WHERE advertiserId = tmpRecord.advertiserId;
            ELSE
              -- Do not charge but increase the advertiser charge count
               UPDATE balance_algo_first_balance SET count_charge_times = (count_charge + 1) WHERE advertiserId = tmpRecord.advertiserId;        
            END IF;   

      END LOOP;
      
     COMMIT;
   
     BEGIN
        balance_first_op_write(query_id);
     END;
   
END balance_first_price;


PROCEDURE balance_second_op_write(query_id IN queries.queryId%TYPE) IS
  
  query_adver_rec QueryAdvertiserMapping%ROWTYPE;
  current_balance balance_algo_second_balance.balance%TYPE;
  current_budget advertisers.budget%TYPE;
  cur_task_limit systemInParams.TASKLIMIT%TYPE;
  
BEGIN
  
  SELECT TASKLIMIT INTO cur_task_limit FROM systemInParams WHERE lower(taskname) like lower('%TASK4%');
  
  FOR query_adver_rec IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id  AND BALANCE_SECOND_RANK!= 0 AND rounded_balance_second <= cur_task_limit ORDER BY rounded_balance_second ASC)
   LOOP
      
      SELECT budget INTO current_budget FROM advertisers WHERE advertiserId = query_adver_rec.advertiserId;
      SELECT balance INTO current_balance FROM balance_algo_second_balance WHERE advertiserid = query_adver_rec.advertiserid;
   
      INSERT INTO balance_second_output (queryid, rank, advertiserId, balance, budget) VALUES (query_id, query_adver_rec.rounded_balance_second, query_adver_rec.advertiserId, current_balance, current_budget);
   
   END LOOP;
   COMMIT;
END balance_second_op_write;


-- This will calculate advertisers for balance second price algorithm
PROCEDURE balance_second_price(query_id IN queries.queryId%TYPE) IS

  query_adver_rec QueryAdvertiserMapping%ROWTYPE;
  current_balance balance_algo_second_balance.balance%TYPE;
  count_charge balance_algo_second_balance.count_charge_times%TYPE;
  ad_rank QueryAdvertiserMapping.balance_second_rank%TYPE;
  hundred_ctc_val advertisers.hundred_ctc%TYPE;
  mod_value number;
  query_adv_rec QueryAdvertiserMapping%ROWTYPE;
  cur_task_limit systemInParams.TASKLIMIT%TYPE;
  bid_val queryadvertisermapping.totalbid%TYPE;
  tmp_count number := 0;
  
BEGIN

     --Find valid advertisers for this particular query on the basis of their current balance.
    FOR valid_adver_rec IN (SELECT queryId, q.advertiserid AS advertiserId FROM QueryAdvertiserMapping q, balance_algo_second_balance g WHERE  q.queryId = query_id AND q.advertiserid = g.advertiserid AND g.balance >= q.totalbid)    
    LOOP
    
      INSERT INTO valid_adver_balance_second (queryId, advertiserId) VALUES (valid_adver_rec.queryId, valid_adver_rec.advertiserId);
    
    END LOOP;

    FOR query_adver_rec IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id)
    LOOP
       
      SELECT balance INTO current_balance FROM balance_algo_second_balance WHERE advertiserid = query_adver_rec.advertiserid;
      --Check if the balance is sufficient to pay for bid
      IF (current_balance >= query_adver_rec.totalBid ) THEN
        --Calculate ad rank on the basis of balance
        ad_rank := (current_balance * query_adver_rec.qualityScore);
  
      ELSE
        -- Put ad rank as 0
        ad_rank := 0;
      
      END IF;

      UPDATE QueryAdvertiserMapping SET BALANCE_SECOND_RANK = ad_rank WHERE queryId = query_id AND advertiserId = query_adver_rec.advertiserId;

    END LOOP;    
      
      current_balance := 0;
      --Now get the actual rank from calculated rank
       FOR rank_rec IN (SELECT advertiserId, RANK() OVER (PARTITION BY queryid ORDER BY BALANCE_SECOND_RANK DESC, advertiserId ASC) as actual_rank FROM QueryAdvertiserMapping WHERE queryid = query_id) 
       LOOP 
          UPDATE QueryAdvertiserMapping SET rounded_balance_second = rank_rec.actual_rank WHERE queryid = query_id AND advertiserId = rank_rec.advertiserId;
       END LOOP;
    
      --Charge only those advertisers who will be within specified ranks
      SELECT TASKLIMIT INTO cur_task_limit FROM systemInParams WHERE TRIM(both from lower(taskname)) like TRIM(both from lower('%TASK4%'));
    
      FOR tmpRecord IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id AND BALANCE_SECOND_RANK!= 0 AND rounded_balance_second <= cur_task_limit ORDER BY rounded_balance_second ASC)
      LOOP
            SELECT hundred_ctc INTO hundred_ctc_val FROM advertisers WHERE advertiserid = tmpRecord.advertiserid;
            SELECT balance, count_charge_times INTO current_balance, count_charge FROM balance_algo_second_balance WHERE advertiserid = tmpRecord.advertiserid;
        
              tmp_count := 0;
              
              SELECT count(q.totalBid) INTO tmp_count FROM queryadvertisermapping q, balance_algo_second_balance g 
              WHERE q.queryId = query_id AND q.totalBid < tmpRecord.totalBid AND q.advertiserid IN (SELECT t.advertiserId FROM valid_adver_balance_second t WHERE t.queryId = query_id)  
              ORDER BY q.totalBid DESC;
              
              IF (tmp_count > 0) THEN
                  -- If there is such bid, then pay by that bid
                  SELECT otherbid INTO bid_val FROM(SELECT q.totalBid as otherbid FROM queryadvertisermapping q, balance_algo_second_balance g 
                  WHERE q.queryId = query_id AND q.totalBid < tmpRecord.totalBid AND q.advertiserid IN (SELECT t.advertiserId FROM valid_adver_balance_second t WHERE t.queryId = query_id)  
                  ORDER BY q.totalBid DESC) WHERE ROWNUM = 1 ;
              
              ELSE
                -- If there is no such bid, then pay by his/her own bid
                bid_val := tmpRecord.totalBid;
              
              END IF;
        
        
            -- Now decide whether to charge this advertiser or not and if we need to charge then what is the amount
            SELECT MOD(count_charge,100) INTO mod_value FROM dual;
     
            IF( mod_value < hundred_ctc_val) THEN
              -- Charge the advertiser
              current_balance := current_balance - bid_val;
              UPDATE balance_algo_second_balance SET balance = current_balance, count_charge_times = (count_charge + 1) WHERE advertiserId = tmpRecord.advertiserId;
            ELSE
              -- Do not charge but increase the advertiser charge count
               UPDATE balance_algo_second_balance SET count_charge_times = (count_charge + 1) WHERE advertiserId = tmpRecord.advertiserId;        
            END IF;   

      END LOOP;
      
     COMMIT;
   
     BEGIN
        balance_second_op_write(query_id);
     END;
   
END balance_second_price;


PROCEDURE generalized_first_op_write(query_id IN queries.queryId%TYPE) IS
  
  query_adver_rec QueryAdvertiserMapping%ROWTYPE;
  current_balance generalized_first_balance.balance%TYPE;
  current_budget advertisers.budget%TYPE;
  cur_task_limit systemInParams.TASKLIMIT%TYPE;
  
BEGIN
  
  SELECT TASKLIMIT INTO cur_task_limit FROM systemInParams WHERE lower(taskname) like lower('%TASK5%');
  
  FOR query_adver_rec IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id  AND GENERALIZED_FIRST_RANK!= 0 AND rounded_generalized_first <= cur_task_limit ORDER BY rounded_generalized_first ASC)
   LOOP
      
      SELECT budget INTO current_budget FROM advertisers WHERE advertiserId = query_adver_rec.advertiserId;
      SELECT balance INTO current_balance FROM generalized_first_balance WHERE advertiserid = query_adver_rec.advertiserid;
   
      INSERT INTO generalized_first_output (queryid, rank, advertiserId, balance, budget) VALUES (query_id, query_adver_rec.rounded_generalized_first, query_adver_rec.advertiserId, current_balance, current_budget);
   
   END LOOP;
   COMMIT;
END generalized_first_op_write;


-- This will calculate advertisers for generalized first price algorithm
PROCEDURE generalized_first_price(query_id IN queries.queryId%TYPE) IS

  query_adver_rec QueryAdvertiserMapping%ROWTYPE;
  current_balance generalized_first_balance.balance%TYPE;
  count_charge generalized_first_balance.count_charge_times%TYPE;
  ad_rank QueryAdvertiserMapping.generalized_first_rank%TYPE;
  hundred_ctc_val advertisers.hundred_ctc%TYPE;
  actual_budget advertisers.budget%TYPE;
  mod_value number;
  query_adv_rec QueryAdvertiserMapping%ROWTYPE;
  cur_task_limit systemInParams.TASKLIMIT%TYPE;
  fraction_budget_var generalized_first_balance.fraction_budget%TYPE;
  tmp_value_exponent float := 0;
  tmp_psi_val float := 0;
  
BEGIN

    FOR query_adver_rec IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id)
    LOOP
       
      SELECT balance, fraction_budget INTO current_balance, fraction_budget_var FROM generalized_first_balance WHERE advertiserid = query_adver_rec.advertiserid;
      --Check if the balance is sufficient to pay for bid
      IF (current_balance >= query_adver_rec.totalBid ) THEN
        --Calculate ad rank on the basis of psi value
        
        SELECT EXP(-1 * fraction_budget_var) INTO tmp_value_exponent FROM dual;
        tmp_psi_val := query_adver_rec.totalBid * (1 - tmp_value_exponent);
        
        ad_rank := (tmp_psi_val * query_adver_rec.qualityScore);
  
      ELSE
        -- Put ad rank as 0
        ad_rank := 0;
      
      END IF;

      UPDATE QueryAdvertiserMapping SET GENERALIZED_FIRST_RANK = ad_rank WHERE queryId = query_id AND advertiserId = query_adver_rec.advertiserId;

    END LOOP;    
      
      current_balance := 0;
      fraction_budget_var := 0;
      
      --Now get the actual rank from calculated rank
       FOR rank_rec IN (SELECT advertiserId, RANK() OVER (PARTITION BY queryid ORDER BY GENERALIZED_FIRST_RANK DESC, advertiserId ASC) as actual_rank FROM QueryAdvertiserMapping WHERE queryid = query_id) 
       LOOP 
          UPDATE QueryAdvertiserMapping SET rounded_generalized_first = rank_rec.actual_rank WHERE queryid = query_id AND advertiserId = rank_rec.advertiserId;
       END LOOP;
    
      --Charge only those advertisers who will be within specified ranks
      SELECT TASKLIMIT INTO cur_task_limit FROM systemInParams WHERE TRIM(both from lower(taskname)) like TRIM(both from lower('%TASK5%'));
    
      FOR tmpRecord IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id AND GENERALIZED_FIRST_RANK!= 0 AND rounded_generalized_first <= cur_task_limit ORDER BY rounded_generalized_first ASC)
      LOOP
            SELECT hundred_ctc, budget INTO hundred_ctc_val, actual_budget FROM advertisers WHERE advertiserid = tmpRecord.advertiserid;
            SELECT balance, count_charge_times INTO current_balance, count_charge FROM generalized_first_balance WHERE advertiserid = tmpRecord.advertiserid;
        
            -- Now decide whether to charge this advertiser or not and if we need to charge then what is the amount
            SELECT MOD(count_charge,100) INTO mod_value FROM dual;
     
            IF( mod_value < hundred_ctc_val) THEN
              -- Charge the advertiser
              current_balance := current_balance - tmpRecord.totalBid;
              UPDATE generalized_first_balance SET balance = current_balance, fraction_budget= (current_balance/actual_budget), count_charge_times = (count_charge + 1) WHERE advertiserId = tmpRecord.advertiserId;
            ELSE
              -- Do not charge but increase the advertiser charge count
               UPDATE generalized_first_balance SET count_charge_times = (count_charge + 1) WHERE advertiserId = tmpRecord.advertiserId;        
            END IF;   

      END LOOP;
      
     COMMIT;
   
     BEGIN
        generalized_first_op_write(query_id);
     END;
   
END generalized_first_price;

PROCEDURE generalized_second_op_write(query_id IN queries.queryId%TYPE) IS
  
  query_adver_rec QueryAdvertiserMapping%ROWTYPE;
  current_balance generalized_second_balance.balance%TYPE;
  current_budget advertisers.budget%TYPE;
  cur_task_limit systemInParams.TASKLIMIT%TYPE;
  
BEGIN
  
  SELECT TASKLIMIT INTO cur_task_limit FROM systemInParams WHERE lower(taskname) like lower('%TASK6%');
  
  FOR query_adver_rec IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id  AND GENERALIZED_SECOND_RANK!= 0 AND ROUNDED_GENERALIZED_SECOND <= cur_task_limit ORDER BY ROUNDED_GENERALIZED_SECOND ASC)
   LOOP
      
      SELECT budget INTO current_budget FROM advertisers WHERE advertiserId = query_adver_rec.advertiserId;
      SELECT balance INTO current_balance FROM generalized_second_balance WHERE advertiserid = query_adver_rec.advertiserid;
   
      INSERT INTO generalized_second_output (queryid, rank, advertiserId, balance, budget) VALUES (query_id, query_adver_rec.ROUNDED_GENERALIZED_SECOND, query_adver_rec.advertiserId, current_balance, current_budget);
   
   END LOOP;
   COMMIT;
END generalized_second_op_write;


-- This will calculate advertisers for generalized second price algorithm
PROCEDURE generalized_second_price(query_id IN queries.queryId%TYPE) IS

  query_adver_rec QueryAdvertiserMapping%ROWTYPE;
  current_balance generalized_second_balance.balance%TYPE;
  count_charge generalized_second_balance.count_charge_times%TYPE;
  ad_rank QueryAdvertiserMapping.generalized_second_rank%TYPE;
  hundred_ctc_val advertisers.hundred_ctc%TYPE;
  actual_budget advertisers.budget%TYPE;
  mod_value number;
  query_adv_rec QueryAdvertiserMapping%ROWTYPE;
  cur_task_limit systemInParams.TASKLIMIT%TYPE;
  fraction_budget_var generalized_second_balance.fraction_budget%TYPE;
  tmp_value_exponent float := 0;
  tmp_psi_val float := 0;
  bid_val queryadvertisermapping.totalbid%TYPE;
  tmp_count number := 0;
  
  
BEGIN

     --Find valid advertisers for this particular query on the basis of their current balance.
    FOR valid_adver_rec IN (SELECT queryId, q.advertiserid AS advertiserId FROM QueryAdvertiserMapping q, generalized_second_balance g WHERE  q.queryId = query_id AND q.advertiserid = g.advertiserid AND g.balance >= q.totalbid)    
    LOOP
    
      INSERT INTO valid_adver_generalized_second (queryId, advertiserId) VALUES (valid_adver_rec.queryId, valid_adver_rec.advertiserId);
    
    END LOOP;
     

    FOR query_adver_rec IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id)
    LOOP
       
      SELECT balance, fraction_budget INTO current_balance, fraction_budget_var FROM generalized_second_balance WHERE advertiserid = query_adver_rec.advertiserid;
      --Check if the balance is sufficient to pay for bid
      IF (current_balance >= query_adver_rec.totalBid ) THEN
        --Calculate ad rank on the basis of psi value
        
        SELECT EXP(-1 * fraction_budget_var) INTO tmp_value_exponent FROM dual;
        tmp_psi_val := query_adver_rec.totalBid * (1 - tmp_value_exponent);
        
        ad_rank := (tmp_psi_val * query_adver_rec.qualityScore);
  
      ELSE
        -- Put ad rank as 0
        ad_rank := 0;
      
      END IF;

      UPDATE QueryAdvertiserMapping SET GENERALIZED_SECOND_RANK = ad_rank WHERE queryId = query_id AND advertiserId = query_adver_rec.advertiserId;

    END LOOP;    
      
      current_balance := 0;
      fraction_budget_var := 0;
      
      --Now get the actual rank from calculated rank
       FOR rank_rec IN (SELECT advertiserId, RANK() OVER (PARTITION BY queryid ORDER BY GENERALIZED_SECOND_RANK DESC, advertiserId ASC) as actual_rank FROM QueryAdvertiserMapping WHERE queryid = query_id) 
       LOOP 
          UPDATE QueryAdvertiserMapping SET ROUNDED_GENERALIZED_SECOND = rank_rec.actual_rank WHERE queryid = query_id AND advertiserId = rank_rec.advertiserId;
       END LOOP;
    
      --Charge only those advertisers who will be within specified ranks
      SELECT TASKLIMIT INTO cur_task_limit FROM systemInParams WHERE TRIM(both from lower(taskname)) like TRIM(both from lower('%TASK6%'));
    
      FOR tmpRecord IN (SELECT * FROM QueryAdvertiserMapping WHERE queryId = query_id AND GENERALIZED_SECOND_RANK!= 0 AND ROUNDED_GENERALIZED_SECOND <= cur_task_limit ORDER BY ROUNDED_GENERALIZED_SECOND ASC)
      LOOP
            SELECT hundred_ctc, budget INTO hundred_ctc_val, actual_budget FROM advertisers WHERE advertiserid = tmpRecord.advertiserid;
            SELECT balance, count_charge_times INTO current_balance, count_charge FROM generalized_second_balance WHERE advertiserid = tmpRecord.advertiserid;
        
            tmp_count := 0;
            
            SELECT count(q.totalBid) INTO tmp_count FROM queryadvertisermapping q, generalized_second_balance g 
            WHERE q.queryId = query_id AND q.totalBid < tmpRecord.totalBid AND q.advertiserid IN (SELECT t.advertiserId FROM valid_adver_generalized_second t WHERE t.queryId = query_id)  
            ORDER BY q.totalBid DESC;
            
            IF (tmp_count > 0) THEN
                -- If there is such bid, then pay by that bid
                SELECT otherbid INTO bid_val FROM(SELECT q.totalBid as otherbid FROM queryadvertisermapping q, generalized_second_balance g 
                WHERE q.queryId = query_id AND q.totalBid < tmpRecord.totalBid AND q.advertiserid IN (SELECT t.advertiserId FROM valid_adver_generalized_second t WHERE t.queryId = query_id)  
                ORDER BY q.totalBid DESC) WHERE ROWNUM = 1 ;
            
            ELSE
              -- If there is no such bid, then pay by his/her own bid
              bid_val := tmpRecord.totalBid;
            
            END IF;
        
            -- Now decide whether to charge this advertiser or not and if we need to charge then what is the amount
            SELECT MOD(count_charge,100) INTO mod_value FROM dual;
     
            IF( mod_value < hundred_ctc_val) THEN
              -- Charge the advertiser
              current_balance := current_balance - bid_val;
              UPDATE generalized_second_balance SET balance = current_balance, fraction_budget= (current_balance/actual_budget), count_charge_times = (count_charge + 1) WHERE advertiserId = tmpRecord.advertiserId;
            ELSE
              -- Do not charge but increase the advertiser charge count
               UPDATE generalized_second_balance SET count_charge_times = (count_charge + 1) WHERE advertiserId = tmpRecord.advertiserId;        
            END IF;   

      END LOOP;
      
     COMMIT;
   
     BEGIN
        generalized_second_op_write(query_id);
     END;
   
END generalized_second_price;


-- This procedure calls all the internal procedures and executes all the algorithms for every query
PROCEDURE parent_procedure IS 
  -- FIXME Remove where part. We want to execute this for all the queries
  CURSOR fetch_queries is SELECT queryId, queryText FROM queries order by queryId ASC;
  query_word queries.queryText%TYPE;
  query_record queries%ROWTYPE;
  
BEGIN

  -- For every query run all the algorithms and store the results into respective tables.
 
    FOR query_record IN fetch_queries
    LOOP
          
       BEGIN
         -- Separate words in the query
         split_query_into_words_new(query_record);         
       END;

       BEGIN
       -- Find out advertisers who bid for at least one word in the query and insert them as mapping
          find_valid_advertisers(query_record.queryId);       
       END;

       BEGIN
       -- Calculate cosine similarity, total bid and quality score for every query advertiser pair
          calculate_basics(query_record.queryId);
       END;

       BEGIN
         -- Run greedy first algorithm
          greedy_first_price(query_record.queryId);
       END;
       
       BEGIN
         -- Run greedy second algorithm
            greedy_second_price(query_record.queryId);
       END; 
       
       BEGIN
        -- Run balance first algorithm
            balance_first_price(query_record.queryId);
       END;       
       
       BEGIN
       -- Run balance second algorithm
           balance_second_price(query_record.queryId);
       END; 
    
       BEGIN
         -- Run generalized first algorithm
            generalized_first_price(query_record.queryId);
       END; 

       BEGIN
         -- Run generalized second algorithm
            generalized_second_price(query_record.queryId);
       END; 

    END LOOP;
    COMMIT;
    
END parent_procedure;


BEGIN
  parent_procedure();
END;
/

exit;
