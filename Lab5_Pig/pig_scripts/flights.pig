-- ============================================
-- Script Pig : Analyse des vols aériens
-- ============================================

-- Charger les données
flights = LOAD '/input/flights/*.csv'
  USING PigStorage(',')
  AS (
    Year:int, Month:int, DayofMonth:int, DayOfWeek:int,
    DepTime:int, CRSDepTime:int, ArrTime:int, CRSArrTime:int,
    UniqueCarrier:chararray, FlightNum:chararray, TailNum:chararray,
    ActualElapsedTime:int, CRSElapsedTime:int, AirTime:int,
    ArrDelay:int, DepDelay:int,
    Origin:chararray, Dest:chararray,
    Distance:int, TaxiIn:int, TaxiOut:int,
    Cancelled:int, CancellationCode:chararray,
    Diverted:int, CarrierDelay:int, WeatherDelay:int,
    NASDelay:int, SecurityDelay:int, LateAircraftDelay:int
  );

-- Filtrer la ligne d'en-tête (si présente)
flights_clean = FILTER flights BY Year IS NOT NULL AND Year > 1900;

-- ============================================
-- Q1: Top 20 aéroports par volume total de vols
-- ============================================

-- Vols au départ
by_origin = GROUP flights_clean BY Origin;
count_origin = FOREACH by_origin GENERATE 
  group AS airport, 
  COUNT(flights_clean) AS nb_departures;

-- Vols à l'arrivée
by_dest = GROUP flights_clean BY Dest;
count_dest = FOREACH by_dest GENERATE 
  group AS airport, 
  COUNT(flights_clean) AS nb_arrivals;

-- Jointure complète
all_counts = JOIN count_origin BY airport FULL OUTER, count_dest BY airport;

-- Calculer le total
all_counts_clean = FOREACH all_counts GENERATE
  (count_origin::airport IS NOT NULL ? count_origin::airport : count_dest::airport) AS airport,
  (count_origin::nb_departures IS NOT NULL ? count_origin::nb_departures : 0L) AS nb_dep,
  (count_dest::nb_arrivals IS NOT NULL ? count_dest::nb_arrivals : 0L) AS nb_arr,
  ((count_origin::nb_departures IS NOT NULL ? count_origin::nb_departures : 0L) + 
   (count_dest::nb_arrivals IS NOT NULL ? count_dest::nb_arrivals : 0L)) AS nb_total;

-- Top 20
top20_airports = LIMIT (ORDER all_counts_clean BY nb_total DESC) 20;

DUMP top20_airports;

-- ============================================
-- Q2: Popularité des transporteurs (volume par année)
-- ============================================

by_carrier_year = GROUP flights_clean BY (UniqueCarrier, Year);
carrier_volume = FOREACH by_carrier_year GENERATE
  FLATTEN(group) AS (carrier, year),
  COUNT(flights_clean) AS volume;

-- Calculer log10 du volume
carrier_volume_log = FOREACH carrier_volume GENERATE
  carrier,
  year,
  volume,
  LOG10((double)volume) AS log_volume;

DUMP carrier_volume_log;

-- ============================================
-- Q3: Proportion de vols retardés (par année)
-- Un vol est retardé si ArrDelay > 15
-- ============================================

flights_with_delay = FOREACH flights_clean GENERATE
  Year,
  (ArrDelay IS NOT NULL AND ArrDelay > 15 ? 1 : 0) AS is_delayed;

by_year = GROUP flights_with_delay BY Year;
delay_ratio_by_year = FOREACH by_year GENERATE
  group AS year,
  COUNT(flights_with_delay) AS total_flights,
  SUM(flights_with_delay.is_delayed) AS delayed_flights,
  (double)SUM(flights_with_delay.is_delayed) / COUNT(flights_with_delay) AS delay_ratio;

DUMP delay_ratio_by_year;

-- ============================================
-- Q4: Retards par transporteur (par année)
-- ============================================

flights_delay_carrier = FOREACH flights_clean GENERATE
  UniqueCarrier,
  Year,
  (ArrDelay IS NOT NULL AND ArrDelay > 15 ? 1 : 0) AS is_delayed;

by_carrier_year_delay = GROUP flights_delay_carrier BY (UniqueCarrier, Year);
delay_by_carrier = FOREACH by_carrier_year_delay GENERATE
  FLATTEN(group) AS (carrier, year),
  COUNT(flights_delay_carrier) AS total_flights,
  SUM(flights_delay_carrier.is_delayed) AS delayed_flights,
  (double)SUM(flights_delay_carrier.is_delayed) / COUNT(flights_delay_carrier) AS delay_ratio;

-- Trier par carrier et year
delay_by_carrier_sorted = ORDER delay_by_carrier BY carrier, year;

DUMP delay_by_carrier_sorted;

-- ============================================
-- Q5: Itinéraires les plus fréquentés
-- Normaliser (Origin, Dest) en paire non ordonnée
-- ============================================

routes = FOREACH flights_clean GENERATE
  (Origin < Dest ? CONCAT(Origin, '-', Dest) : CONCAT(Dest, '-', Origin)) AS route;

routes_grouped = GROUP routes BY route;
routes_freq = FOREACH routes_grouped GENERATE
  group AS route,
  COUNT(routes) AS frequency;

-- Top 20 itinéraires
top20_routes = LIMIT (ORDER routes_freq BY frequency DESC) 20;

-- Enregistrer le résultat final
STORE top20_routes INTO '/pigout/top_routes'
  USING PigStorage(',');