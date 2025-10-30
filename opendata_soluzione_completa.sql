
/* landing zone 

-- creo lo schema della landing zone: da eseguire solo una volta e quindi da tenere commentato

create schema openbo_landing;

*/


-- integration layer 

-- creo lo schema dell'integration layer

drop schema if exists openbo_integration cascade;
create schema openbo_integration;

set search_path to openbo_integration;

-- identifico errori nelle date per incarichi di collaborazione
drop table if exists et_incarichi_collaborazione;

create table et_incarichi_collaborazione as
-- seleziono gli errori in cui data fine minore di data inizio
select *, 'warning: data fine minore di data inizio' as error_code
from openbo_landing."lt_incarichi_di_collaborazione"
where "durata_dal">"durata_al"
union all
-- seleziono gli errori in cui il responsabile e' mancante
select *, 'warning: responsabile mancante' as error_code
from openbo_landing."lt_incarichi_di_collaborazione"
where trim("responsabile") is null or "responsabile"='' -- si potrebbe anche scrivere coalesce("responsabile", '') = '' ma la lettura risulta meno intuitiva. ndAle qui metto il trim perche' potrebbe esserci uno spaz
union all
-- seleziono gli errori in cui l'importo e' null
select *, 'error: importo mancante' as error_code
from openbo_landing."lt_incarichi_di_collaborazione"
where "importo" is null
;

-- identifico errori nelle date per incarichi conferiti

drop table if exists et_incarichi_conferiti;

create table et_incarichi_conferiti as
-- seleziono gli errori in cui data fine minore di data inizio
select *, 'warning: data fine minore di data inizio' as error_code
from openbo_landing."lt_incarichi_conferiti"
where "data_inizio_incarico">"data_fine_incarico"
union all
-- seleziono gli errori in cui il responsabile e' mancante
select *, 'warning: responsabile mancante' as error_code
from openbo_landing."lt_incarichi_conferiti"
where "responsabile_della_struttura_conferente" is null or "responsabile_della_struttura_conferente"=''
union all
-- seleziono gli errori in cui l'importo e' null
select *, 'error: importo mancante' as error_code
from openbo_landing."lt_incarichi_conferiti"
where "compenso_previsto" is null;


-- creo una tabella temporanea con i nomi delle colonne target di incarichi di collaborazione
drop table if exists tt_incarichi_collaborazione;

create table tt_incarichi_collaborazione as
select row_number() over() as ids_riga,
"id" as id_incarico,
"n_pg_atto" as numero_pg_atto,
"anno_pg_atto" as anno_pg_atto,
"oggetto" as oggetto,
"classificazione_incarichi" as id_classificazione_incarico,
"descrizione_classificazione_incarichi" descrizione_classificazione_incarico,
"norma_o_titolo" as norma_titolo_base,
"importo"::decimal as importo,
initcap("settore_dipartimento_area") as nome_struttura,
-- "servizio", --campo da trascurare
-- "uo", --campo da trascurare
-- "dirigente", --campo da trascurare
initcap("responsabile") as nominativo_responsabile,
initcap("ragione_sociale") as ragione_sociale,
"partita_iva" as partita_iva,
"codice_fiscale" as codice_fiscale,
"durata_dal" as giorno_inizio,
"durata_al" as giorno_fine,
--"curriculum_link" --campo da trascurare
'incarichi di collaborazione' as source_system,
now() as load_timestamp
from openbo_landing."lt_incarichi_di_collaborazione"
where "importo" is not null; -- come da specifica scarto le righe con importo nullo

-- creo una tabella temporanea con i nomi delle colonne target di incarichi conferiti
drop table if exists tt_incarichi_conferiti;

create table tt_incarichi_conferiti as
select row_number() over() as ids_riga,
"id" as id_incarico,
"n_pg_atto" as numero_pg_atto,
"anno_pg_atto" as anno_pg_atto,
"classificazione_incarico" as id_classificazione_incarico,
-- "descrizione_incarico", --campo da trascurare
"data_inizio_incarico" as giorno_inizio,
"data_fine_incarico" as giorno_fine,
-- "durata_incarico", --campo da trascurare: andra' ricalcolato
"compenso_previsto"::decimal as importo,
initcap("struttura_conferente") as nome_struttura,
initcap("responsabile_della_struttura_conferente") as nominativo_responsabile,
'incarichi conferiti' as source_system,
now() as load_timestamp
from openbo_landing."lt_incarichi_conferiti"
where "compenso_previsto" is not null; -- come da specifica scarto le righe con importo nullo


-- creo la tabella temporanea degli atti da incarichi di collaborazione
/* in questo caso avendo diverse colonne costruisco 
 * la dimensione con dei passaggi intermedi e non in unica query
 * perche' usando subquery il codice sarebbe di difficile lettura
*/

drop table if exists tt_dim_atto_incarichi_collaborazione;

create table tt_dim_atto_incarichi_collaborazione as
--per semplicita' si e' usato il max invece di raggruppare per tutti i valori in modo anche da avere sicurezza di unicita' di chiave
select numero_pg_atto, max(anno_pg_atto) as anno_pg_atto, max(oggetto) as oggetto, max(norma_titolo_base) as norma_titolo_base, max(source_system) as source_system
from tt_incarichi_collaborazione
where numero_pg_atto is not null
group by numero_pg_atto;

/* nota: la semplificazione di usare il max puo' incontrare errori 
 * se la chiave fosse multipla e anno_pg_atto non fosse univoco
 * per verificarlo si e' ad esempio visto nella fase di data profiling
 * che le seguenti query restituiscono lo stesso valore distinct di atti (525)

 * ipotesi doppia chiave numero_pg_atto, anno_pg_atto

select count(*)
from 
(select numero_pg_atto, anno_pg_atto
from tt_incarichi_collaborazione
where numero_pg_atto is not null
group by numero_pg_atto, anno_pg_atto)

 * ipotesi singola chiave numero_pg_atto

select count(*)
from 
(select numero_pg_atto
from tt_incarichi_collaborazione
where numero_pg_atto is not null
group by numero_pg_atto)

 * e l'ipotesi della singola chiave numero_pg_atto e' verificata
 *  */

-- creo la tabella temporanea degli atti da incarichi conferiti
drop table if exists tt_dim_atto_incarichi_conferiti;

/* nb: i dati sono comunque disgiunti,
 * ossia gli atti di incarichi conferiti
 * non sono mai gli stessi di incarichi di collaborazione.
 * ad ogni modo si segue la specifica data dalla business rule in casi come questi
 * perche' i dati potrebbero cambiare nel futuro e sovrapporsi.
 */

create table tt_dim_atto_incarichi_conferiti as
select a.numero_pg_atto, max(a.anno_pg_atto) as anno_pg_atto, max(a.source_system) as source_system --per semplicita' si e' usato il max invece di raggruppare per tutti i valori in modo anche da avere sicurezza di unicita' di chiave
from tt_incarichi_conferiti a
left join tt_dim_atto_incarichi_collaborazione b -- in alternativa al left join in cui poi si verifica che siano disgiunti con "is null" si puo' procedere con una group by. useremo questa modalita' per un altra dimensione
on a.numero_pg_atto=b.numero_pg_atto
where a.numero_pg_atto is not null
and b.numero_pg_atto is null -- escludo i dati corrispondenti a dei record nella tabella a destra del join e quindi in tt_dim_atto_incarichi_collaborazione
group by a.numero_pg_atto;


-- creo la tabella dimensionale degli atti
drop table if exists it_dim_atto;

create table it_dim_atto as
select row_number() over() as ids_atto, numero_pg_atto, anno_pg_atto, oggetto, norma_titolo_base, source_system
from
	(
	select numero_pg_atto, anno_pg_atto, oggetto, norma_titolo_base, source_system
	from tt_dim_atto_incarichi_collaborazione
	union
	select numero_pg_atto, anno_pg_atto, null as oggetto, null as norma_titolo_base, source_system
	from tt_dim_atto_incarichi_conferiti
	);

-- inserisco un fittizio in atto

insert into it_dim_atto (ids_atto, numero_pg_atto, anno_pg_atto, oggetto, norma_titolo_base, source_system)
values(-1, null, null, '*** atto fittizio', null, 'etl');

-- creo la dimensione classificazione incarico

drop table if exists it_dim_classificazione_incarico;

create table it_dim_classificazione_incarico as
select row_number() over() as ids_classificazione_incarico, id_classificazione_incarico, max(descrizione_classificazione_incarico) as descrizione_classificazione_incarico, max(source_system) as source_system
from tt_incarichi_collaborazione
group by id_classificazione_incarico;

-- inserisco un fittizio

insert into openbo_integration.it_dim_classificazione_incarico
(ids_classificazione_incarico, id_classificazione_incarico, descrizione_classificazione_incarico, source_system)
values(-1, null, '*** classificazione fittizia', 'etl');


-- creo la tabella temporanea delle strutture da incarichi di collaborazione
drop table if exists it_dim_struttura;

/* in questo caso avendo una sola colonna costruisco 
 * la dimensione con un unica query usando subquery in quanto
 * di semplice lettura
*/
create table it_dim_struttura as
select row_number() over() as ids_struttura, nome_struttura, source_system
from 
	(
		(
		select nome_struttura, max(source_system) as source_system --source_system ha sempre lo stesso valore, si potrebbe mettere anche nel group by
		from tt_incarichi_collaborazione
		where nome_struttura is not null
		group by nome_struttura
		)
	union
		(
		select a.nome_struttura, max(a.source_system) as source_system
		from tt_incarichi_conferiti a
		left join tt_incarichi_collaborazione b
		on a.nome_struttura=b.nome_struttura
		where a.nome_struttura is not null
		and b.nome_struttura is null
		group by a.nome_struttura
		)
	)
;

-- inserisco un fittizio
insert into it_dim_struttura
(ids_struttura, nome_struttura, source_system)
values(-1, '*** struttura fittizia', 'etl');

-- creo la dimensione classificazione incarico

drop table if exists it_dim_soggetto_incaricato;

create table it_dim_soggetto_incaricato as
select row_number() over() as ids_soggetto_incaricato, ragione_sociale, max(partita_iva) as partita_iva, max(codice_fiscale) as codice_fiscale, max(source_system) as source_system --in questo caso l'uso dei max puo' portare a enormi semplificazioni ma si segue la specifica
from tt_incarichi_collaborazione
group by ragione_sociale;

/* si puo' verificare che le ragioni sociali compaiono con valori di partita iva e codice fiscali differenti

select count (*)
from
(
select ragione_sociale, coalesce(partita_iva,'***missing') as partita_iva, coalesce(codice_fiscale,'***missing') as codice_fiscale
from tt_incarichi_collaborazione
group by ragione_sociale, coalesce(partita_iva,'***missing'), coalesce(codice_fiscale,'***missing')
);
-- restituisce 517


-- mentre:
select count (*)
from
(
select ragione_sociale
from tt_incarichi_collaborazione
group by ragione_sociale
);
-- restituisce 494

-- nello specifico alcuni casi in cui i record differiscono sono:

select *
from
(
select ragione_sociale, coalesce(partita_iva,'***missing') as partita_iva, coalesce(codice_fiscale,'***missing') as codice_fiscale
from tt_incarichi_collaborazione
group by ragione_sociale, coalesce(partita_iva,'***missing'), coalesce(codice_fiscale,'***missing')
) a
left join
(
select ragione_sociale, coalesce(partita_iva,'***missing') as partita_iva, coalesce(codice_fiscale,'***missing') as codice_fiscale
from tt_incarichi_collaborazione
group by ragione_sociale, coalesce(partita_iva,'***missing'), coalesce(codice_fiscale,'***missing')
) b
on a.ragione_sociale=b.ragione_sociale
left join it_dim_soggetto_incaricato c
on a.ragione_sociale=c.ragione_sociale
where (a.partita_iva<>b.partita_iva
or a.codice_fiscale<>b.codice_fiscale);

 * da cui si vede che spesso sono informazioni incomplete 
 * di partite iva o codici fiscali mancanti che si sistemano
 * con un max. si vedono anche casi di inserimenti con errori 
 * come esempio codici fiscali leggermente differenti il cui
 * risultato con un max e' casuale ma non risolvibile senza un
 * calcolatore di codici fiscali
 * 
 * ci sarebbe anche la possibilita' di ridurre le occorrenze escludendo
 * certi prefissi come avv. etc. ma con il rischio di commettere errori
 * per cui per semplicita' si sono tenuti cosi'
*/

-- inserisco fittizio
insert into it_dim_soggetto_incaricato
(ids_soggetto_incaricato, ragione_sociale, partita_iva, codice_fiscale, source_system)
values(-1, '*** soggetto fittizio', null, null, 'etl');

-- creo una tabella temporanea per il responsabile da usare per poi passare al risultato finale

-- per poter poi fare sia il mapping che gestire come da requisito la tracciabilita'
drop table if exists tt_dim_responsabile;

create table tt_dim_responsabile as
select
    trim( -- eseguo un trim per rimuovere spazi iniziali e finali
        case
            when substring(nominativo_responsabile, 1, 4) = 'ing.' -- verifico se i primi 4 caratteri sono "ing."
                then substring(nominativo_responsabile, 5) -- escludo i primi 4 e seleziono solo dal 5 in poi
            when substring(nominativo_responsabile, 1, 5) = 'arch.' 
                then substring(nominativo_responsabile, 6)
            when substring(nominativo_responsabile, 1, 4) = 'avv.' 
                then substring(nominativo_responsabile, 5)
            when substring(nominativo_responsabile, 1, 4) = 'avv,' 
                then substring(nominativo_responsabile, 5)
            when substring(nominativo_responsabile, 1, 8) = 'avvocato' 
                then substring(nominativo_responsabile, 9)
            when substring(nominativo_responsabile, 1, 30) = 'il direttore del settore dott.' 
                then substring(nominativo_responsabile, 31)
            when substring(nominativo_responsabile, 1, 31) = 'direttore settore entrate dott.' 
                then substring(nominativo_responsabile, 32)
            when substring(nominativo_responsabile, 1, 8) = 'dott.ssa' 
                then substring(nominativo_responsabile, 9)
            when substring(nominativo_responsabile, 1, 5) = 'dott.' 
                then substring(nominativo_responsabile, 6)
            when substring(nominativo_responsabile, 1, 6) = 'dr.ssa' 
                then substring(nominativo_responsabile, 7)
            when substring(nominativo_responsabile, 1, 3) = 'dr.' 
                then substring(nominativo_responsabile, 4)
            else nominativo_responsabile
        end
    ) as nominativo_responsabile, -- sto creando una tabella di mapping tra vechcioe e nuovo nominativo
   nominativo_responsabile as nominativo_responsabile_originale, 
   source_system
from
	(
	select nominativo_responsabile, source_system
	from tt_incarichi_collaborazione
	union
	select nominativo_responsabile, source_system
	from tt_incarichi_conferiti
	)
where trim(nominativo_responsabile) !=''; -- escludo le stringhe vuote

-- alternativa usando una espressione regolare

create table tt_dim_responsabile as
select
    trim( -- eseguo un trim per rimuovere spazi iniziali e finali
        regexp_replace(
            nominativo_responsabile,
            'arch\.|avv\.|avv,|avvocato|direttore settore entrate dott\.|dott\.|dott\.ssa|dr\.|dr\.ssa|il direttore del settore dott\.|ing\.',
            '',
            'gi'
        )
    ) as nominativo_responsabile, nominativo_responsabile as nominativo_responsabile_originale, source_system
from
	(
	select nominativo_responsabile, source_system
	from tt_incarichi_collaborazione
	union
	select nominativo_responsabile, source_system
	from tt_incarichi_conferiti
	)
where trim(nominativo_responsabile) !='';



-- parte facoltativa: riduciamo i nomi parziali e i ruoli ad interim (a.i.) associandoli allo stesso nome

drop table if exists tt_dim_responsabile_v2;

create table tt_dim_responsabile_v2 as
select 
	trim( -- eseguo un trim per rimuovere spazi iniziali e finali
		case
            when right(nominativo_responsabile, 3) = 'a.i' -- verifico se gli ultimi 3 caratteri sono "a.i"
                then substring(nominativo_responsabile, 1, length(nominativo_responsabile)-3) -- escludo gli ultimi 3 caratteri
            when right(nominativo_responsabile, 4) = 'a.i.' 
                then substring(nominativo_responsabile, 1, length(nominativo_responsabile)-4)
            when nominativo_responsabile = 'labriola' 
                then 'ada labriola'
             when nominativo_responsabile = 'ada simona labriola'
             	then 'ada labriola'
             when nominativo_responsabile = 'anronella trentini'
             	then 'antonella trentini'
             when nominativo_responsabile = 'trentini antonella'
             	then 'antonella trentini'
             when nominativo_responsabile = 'bruni raffela'
             	then 'bruni raffaela'
             when nominativo_responsabile = 'cattoli monica'
             	then 'monica cattoli'
             when nominativo_responsabile = 'cazzola lorenzo'
             	then 'lorenzo cazzola'
             when nominativo_responsabile = 'chirs tomesani'
             	then 'chris tomesani'
             when nominativo_responsabile = 'daniela gemell'
             	then 'daniela gemelli'
             when nominativo_responsabile = 'gemelli daniela'
             	then 'daniela gemelli'
             when nominativo_responsabile = 'fanco chiarini'
             	then 'franco chiarini'
             when nominativo_responsabile = 'franco chiarii'
             	then 'franco chiarini'
             when nominativo_responsabile = 'franco evangelisti'
             	then 'francesco evangelisti'
             when nominativo_responsabile = 'giulia carstia'
             	then 'giulia carestia'
             when nominativo_responsabile = 'mariagrazia bonzagbi'
             	then 'mariagrazia bonzagni'
             when nominativo_responsabile = 'mariagrazioa bonzagni'
             	then 'mariagrazia bonzagni'
             when nominativo_responsabile = 'bonzagni mariagrazia'
             	then 'mariagrazia bonzagni'
             when nominativo_responsabile = 'garifo katiuscia'
             	then 'katiuscia garifo'
             when nominativo_responsabile = 'muzzi mauro'
             	then 'mauro muzzi'
            else nominativo_responsabile
        end
	) as nominativo_responsabile, nominativo_responsabile_originale, source_system
from tt_dim_responsabile;

/* creo una tabella di mapping con le occorrenze distinct 
 * per fare il mapping dei responsabili che servira' 
 * per ricondurre i fatti all'anagrafica in seguito
 */
drop table if exists mt_mapping_responsabile;

create table mt_mapping_responsabile as
select distinct nominativo_responsabile, nominativo_responsabile_originale --importante il distinct altrimenti i dati si moltiplicano facendo il mapping!
from tt_dim_responsabile_v2;


-- creo la dimensione responsabile

drop table if exists it_dim_responsabile;

create table it_dim_responsabile as
select row_number() over() as ids_responsabile, nominativo_responsabile, source_system
from
	(
	/* la specifica indica di dare priorita' a 'incarichi di collaborazione'
	 * rispetto a 'incarichi conferiti'. come nel caso della dimensione dim atto
	 * si potrebbe usare un left join. visto che
	 * la stringa 'incarichi di collaborazione'>'incarichi conferiti'
 	 * useremo in questo caso il trucco di fare un group by e poi scegliere il max:
 	 * se presente in entrambi risultera' 'incarichi di collaborazione'
 	 * se presente solo in 'incarichi conferiti' dara' come risultato quest'ultimo
 	 */
	select nominativo_responsabile, max(source_system) as source_system
	from tt_dim_responsabile_v2
	group by nominativo_responsabile
	order by 1
	);

-- inserisco un fittizio
insert into it_dim_responsabile
(ids_responsabile, nominativo_responsabile, source_system)
values(-1, '*** responsabile fittizio', 'etl');

-- imposto il locale a italiano cosi' da avere le descrizioni dei mesi e dei giorni della settimana in italiano
set lc_time = 'it_IT.UTF-8'; --qui maiuscole e minsi necessita

-- creo la dimensione tempo inizio (si potrebbero creare assieme inizio e fine e selezionare solo le occorrenze utili)

drop table if exists it_dim_tempo_inizio;

create table it_dim_tempo_inizio as
select
	(extract(year from giorno_inizio)*10000+extract(month from giorno_inizio)*100+extract(day from giorno_inizio))::int as ids_giorno, -- importante fare il cast a int
	giorno_inizio as giorno,
	extract(month from giorno_inizio)::int as mese,
	extract(year from giorno_inizio)::int as anno,
	to_char(giorno_inizio, 'tmmonth') as nome_mese,
	'q' || extract(quarter from giorno_inizio) as trimestre, -- calcolo il trimestre in formato q1, q2, q3, q4
	to_char(giorno_inizio, 'tmday') as giorno_settimana,
	'etl' as source_system
from 
	(
	select giorno_inizio
	from tt_incarichi_collaborazione
	union
	select giorno_inizio
	from tt_incarichi_conferiti
	)
where giorno_inizio is not null
order by giorno_inizio;

-- inserisco un fittizio
insert into it_dim_tempo_inizio
(ids_giorno, giorno, mese, anno, nome_mese, trimestre, giorno_settimana, source_system)
values(-1, date '1900-01-01', 1, 1900, '*** mese fittizio', '*** trimestre fittizio', '*** giorno fittizio', 'etl');

-- creo la dimensione tempo fine

drop table if exists it_dim_tempo_fine;

create table it_dim_tempo_fine as
select
	(extract(year from giorno_fine)*10000+extract(month from giorno_fine)*100+extract(day from giorno_fine))::int as ids_giorno_fine,
	giorno_fine,
	extract(month from giorno_fine)::int as mese_fine,
	extract(year from giorno_fine)::int as anno_fine,
	to_char(giorno_fine, 'tmmonth') as nome_mese_fine,
	'q' || extract(quarter from giorno_fine) as trimestre_fine,
	to_char(giorno_fine, 'tmday') as giorno_settimana_fine,
	'etl' as source_system
from 
	(
	select giorno_fine
	from tt_incarichi_collaborazione
	union
	select giorno_fine
	from tt_incarichi_conferiti
	)
where giorno_fine is not null
order by giorno_fine;

-- inserisco un fittizio
insert into it_dim_tempo_fine
(ids_giorno_fine, giorno_fine, mese_fine, anno_fine, nome_mese_fine, trimestre_fine, giorno_settimana_fine, source_system)
values(-1, date '1900-01-01', 1, 1900, '*** mese fittizio', '*** trimestre fittizio', '*** giorno fittizio', 'etl');

-- creo una tabella per il fatto su cui poter fare test e in cui lasciamo colonne che poi non porteremo nel data mart

drop table if exists it_fact_incarichi;

create table it_fact_incarichi as
select
	fi.ids_riga, -- mantengo il campo per controlli: insieme a source_system fornisce una chiave per ogni riga
	fi.id_incarico,
	coalesce(ds.ids_struttura,-1) as ids_struttura, --se non fa match associo a fittizio
	coalesce(dci.ids_classificazione_incarico,-1) as ids_classificazione_incarico, --se non fa match associo a fittizio
	coalesce(extract(year from giorno_inizio)*10000+extract(month from giorno_inizio)*100+extract(day from giorno_inizio),-1)::int as ids_giorno, --se non fa match associo a fittizio
	coalesce(extract(year from giorno_fine)*10000+extract(month from giorno_fine)*100+extract(day from giorno_fine),-1)::int as ids_giorno_fine, --se non fa match associo a fittizio
	coalesce(dsi.ids_soggetto_incaricato,-1) as ids_soggetto_incaricato, --se non fa match associo a fittizio
	coalesce(dr.ids_responsabile,-1) as ids_responsabile, --se non fa match associo a fittizio
	coalesce(da.ids_atto,-1) as ids_atto, --se non fa match associo a fittizio
	fi.importo,
	--calcolo la durata
	case
		when fi.giorno_inizio>fi.giorno_fine
			then null
		else (fi.giorno_fine-fi.giorno_inizio)
	end as durata_giorni,
	-- mantengo i campi nell'integration layer per verifiche future che non importeremo nel data mart
	fi.numero_pg_atto,
	fi.anno_pg_atto,
	fi.id_classificazione_incarico_originale,
	fi.id_classificazione_incarico,
	fi.nome_struttura,
	fi.giorno_inizio,
	fi.giorno_fine,
	fi.nominativo_responsabile,
	fi.ragione_sociale,
	fi.source_system,
	fi.load_timestamp
from
	(
	select ids_riga, id_incarico, numero_pg_atto, anno_pg_atto, id_classificazione_incarico as id_classificazione_incarico_originale,
		-- come da specifica associo a z5 se trovo z1
		case
			when id_classificazione_incarico = 'z1' 
				then 'z5'
			else id_classificazione_incarico
		end as id_classificazione_incarico,	
		nome_struttura, giorno_inizio, giorno_fine, nominativo_responsabile, ragione_sociale, importo, source_system, load_timestamp
	from tt_incarichi_collaborazione
	union all -- attenzione quando si tratta di fatti si fanno sempre union all, non vanno persi dati
	select ids_riga, id_incarico, numero_pg_atto, anno_pg_atto, id_classificazione_incarico as id_classificazione_incarico_originale,
		-- come da specifica associo a z5 se trovo z1
		case
			when id_classificazione_incarico = 'z1' 
				then 'z5'
			else id_classificazione_incarico
		end as id_classificazione_incarico,	
		nome_struttura, giorno_inizio, giorno_fine, nominativo_responsabile, null as ragione_sociale, importo, source_system, load_timestamp
	from tt_incarichi_conferiti
	) fi
-- non faccio join sul tempo perche' non avendo scartato dati non serve (potrei aver pensato regole tipo scartare date antecedenti al 1950)
left join it_dim_struttura ds
on fi.nome_struttura=ds.nome_struttura
left join it_dim_classificazione_incarico dci
on fi.id_classificazione_incarico=dci.id_classificazione_incarico
left join it_dim_soggetto_incaricato dsi
on fi.ragione_sociale=dsi.ragione_sociale
left join it_dim_atto da
on fi.numero_pg_atto=da.numero_pg_atto
--per andare in join sulla dimensione responsabile devo usare la tabella di mapping intermedia e fare un doppio join
left join mt_mapping_responsabile mr
on fi.nominativo_responsabile=mr.nominativo_responsabile_originale --qui non serve indicare "mr." ma e' buona prassi farlo per rendere il join piu' velocemente leggibile
left join it_dim_responsabile dr
on mr.nominativo_responsabile=dr.nominativo_responsabile
;




-- presentation layer: dwh

-- creo il data mart

drop schema if exists openbo_dwh cascade;
create schema openbo_dwh;

set search_path to openbo_dwh;

-- creo il fatto incarichi
drop table if exists fact_incarichi;

create table fact_incarichi as
-- non seleziono tutte le colonne ma solo quelle da specifica
select ids_riga, id_incarico, ids_struttura, ids_classificazione_incarico, ids_giorno, ids_giorno_fine, ids_soggetto_incaricato, ids_responsabile, ids_atto, importo, durata_giorni, source_system, load_timestamp
from openbo_integration.it_fact_incarichi;


-- creo la dimensione atto
drop table if exists dim_atto;

create table dim_atto as
select ids_atto, numero_pg_atto, anno_pg_atto, oggetto, norma_titolo_base, source_system
from openbo_integration.it_dim_atto
-- verifico con una in sul fatto per eliminare occorrenze non utili
where ids_atto in
	(
	select ids_atto from fact_incarichi
	);

-- creo la dimensione classificazione incarico
drop table if exists dim_classificazione_incarico;

create table dim_classificazione_incarico as
select ids_classificazione_incarico, id_classificazione_incarico, descrizione_classificazione_incarico, source_system
from openbo_integration.it_dim_classificazione_incarico
-- verifico con una in sul fatto per eliminare occorrenze non utili
where ids_classificazione_incarico in
	(
	select ids_classificazione_incarico from fact_incarichi
	);

-- creo la dimensione struttura
drop table if exists dim_struttura;

create table dim_struttura as
select ids_struttura, nome_struttura, source_system
from openbo_integration.it_dim_struttura
-- verifico con una in sul fatto per eliminare occorrenze non utili
where ids_struttura in
	(
	select ids_struttura from fact_incarichi
	);

-- creo la dimensione soggetto incaricato
drop table if exists dim_soggetto_incaricato;

create table dim_soggetto_incaricato as
select ids_soggetto_incaricato, ragione_sociale, partita_iva, codice_fiscale, source_system
from openbo_integration.it_dim_soggetto_incaricato
-- verifico con una in sul fatto per eliminare occorrenze non utili
where ids_soggetto_incaricato in
	(
	select ids_soggetto_incaricato from fact_incarichi
	);

-- creo la dimensione responsabile
drop table if exists dim_responsabile;

create table dim_responsabile as
select ids_responsabile, nominativo_responsabile, source_system
from openbo_integration.it_dim_responsabile
-- verifico con una in sul fatto per eliminare occorrenze non utili
where ids_responsabile in
	(
	select ids_responsabile from fact_incarichi
	);


-- creo la dimensione tempo inizio
drop table if exists dim_tempo_inizio;

create table dim_tempo_inizio as
select ids_giorno, giorno, mese, anno, nome_mese, trimestre, giorno_settimana, source_system
from openbo_integration.it_dim_tempo_inizio
-- verifico con una in sul fatto per eliminare occorrenze non utili
where ids_giorno in
	(
	select ids_giorno from fact_incarichi
	);

-- creo la dimensione tempo fine
drop table if exists dim_tempo_fine;

create table dim_tempo_fine as
select ids_giorno_fine, giorno_fine, mese_fine, anno_fine, nome_mese_fine, trimestre_fine, giorno_settimana_fine, source_system
from openbo_integration.it_dim_tempo_fine
-- verifico con una in sul fatto per eliminare occorrenze non utili
where ids_giorno_fine in
	(
	select ids_giorno_fine from fact_incarichi
	);

/*
* eseguo i test di quadratura richiesti
*/
drop table if exists check_importo;

create table check_importo as
select sum("compenso_previsto"::decimal) as importo, 'incarichi conferiti' as source_system, 'openbo_landing' as layer
from openbo_landing."lt_incarichi_conferiti"
union
select sum("importo"::decimal) as importo, 'incarichi di collaborazione' as source_system, 'openbo_landing' as layer
from openbo_landing."lt_incarichi_di_collaborazione"
union
select sum(importo) as importo, source_system, 'openbo_integration' as layer
from openbo_integration.it_fact_incarichi
group by source_system
union
select sum(importo) as importo, source_system, 'openbo_dwh' as layer
from
	(
	select importo, fi.source_system
	from openbo_dwh.fact_incarichi fi
	join openbo_dwh.dim_struttura ds
	on fi.ids_struttura=ds.ids_struttura
	join openbo_dwh.dim_classificazione_incarico dci
	on fi.ids_classificazione_incarico=dci.ids_classificazione_incarico
	join openbo_dwh.dim_soggetto_incaricato dsi
	on fi.ids_soggetto_incaricato=dsi.ids_soggetto_incaricato
	join openbo_dwh.dim_atto da
	on fi.ids_atto=da.ids_atto
	join openbo_dwh.dim_responsabile dr
	on fi.ids_responsabile=dr.ids_responsabile
	join openbo_dwh.dim_tempo_inizio dti
	on fi.ids_giorno=dti.ids_giorno
	join openbo_dwh.dim_tempo_fine dtf
	on fi.ids_giorno_fine=dtf.ids_giorno_fine
	)
group by source_system
;


/*
 * quello che segue serve solo a graficare meglio lo schema

alter table openbo_dwh.dim_atto
add constraint dim_atto_pkey primary key  (ids_atto);

alter table openbo_dwh.dim_classificazione_incarico
add constraint dim_classificazione_incarico_pkey primary key  (ids_classificazione_incarico);

alter table openbo_dwh.dim_responsabile
add constraint dim_responsabile_pkey primary key  (ids_responsabile);

alter table openbo_dwh.dim_soggetto_incaricato
add constraint dim_soggetto_incaricato_pkey primary key  (ids_soggetto_incaricato);

alter table openbo_dwh.dim_struttura
add constraint dim_struttura_pkey primary key  (ids_struttura);

alter table openbo_dwh.dim_tempo_inizio
add constraint dim_tempo_inizio_pkey primary key  (ids_giorno);

alter table openbo_dwh.dim_tempo_fine
add constraint dim_tempo_fine_pkey primary key  (ids_giorno_fine);

alter table openbo_dwh.fact_incarichi add constraint fact_incarichi1_fkey
    foreign key (ids_atto) references openbo_dwh.dim_atto (ids_atto) on delete no action on update no action;

alter table openbo_dwh.fact_incarichi add constraint fact_incarichi2_fkey
    foreign key (ids_classificazione_incarico) references openbo_dwh.dim_classificazione_incarico (ids_classificazione_incarico) on delete no action on update no action;

alter table openbo_dwh.fact_incarichi add constraint fact_incarichi3_fkey
    foreign key (ids_responsabile) references openbo_dwh.dim_responsabile (ids_responsabile) on delete no action on update no action;

alter table openbo_dwh.fact_incarichi add constraint fact_incarichi4_fkey
    foreign key (ids_soggetto_incaricato) references openbo_dwh.dim_soggetto_incaricato (ids_soggetto_incaricato) on delete no action on update no action;

alter table openbo_dwh.fact_incarichi add constraint fact_incarichi5_fkey
    foreign key (ids_struttura) references openbo_dwh.dim_struttura (ids_struttura) on delete no action on update no action;

alter table openbo_dwh.fact_incarichi add constraint fact_incarichi6_fkey
    foreign key (ids_giorno) references openbo_dwh.dim_tempo_inizio (ids_giorno) on delete no action on update no action;

alter table openbo_dwh.fact_incarichi add constraint fact_incarichi7_fkey
    foreign key (ids_giorno_fine) references openbo_dwh.dim_tempo_fine (ids_giorno_fine) on delete no action on update no action;

 */
