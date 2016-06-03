# hackaton-iot

Vous trouverez dans ce repo le code que j'ai écrit pour le hackaton IOT organisé en mai 2016.

## Problème

Un endpoint HTTP reçoit des messages JSON, un par transaction HTTP. Chaque message contient un id, un sensorType, un timestamp et une valeur.
Un autre endpoint fournit des synthèses, avec un timestamp de début et une durée en secondes. Pour chaque sensorType, on doit renvoyer le min/max/avg des value.

## Ordres de grandeur

L'objectif étant d'être le plus rapide, il suffit de minimiser les tâches les plus coûteuses...

 - HTTP
   - Création d'une connexion TCP
   - Tansfert réseau
   - Analyse HTTP
 - Disque
   - Création d'un fichier
   - Lecture/écriture
   - Sérialisation
   - Partitionnement
 - Mémoire
   - Allocation de mémoire
   - Garbage collector
 - CPU
   - Parsing JSON
   - Aggrégation des données

## Choix initiaux

Mon choix de langage s'est porté sur Java 8.
Dès le début, j'ai découplé la problématique du serveur HTTP du traitement des requêtes.

### Serveur HTTP

Afin de ne pas recréer une connexion HTTP pour chaque message. On utilise le header [`Connection: keep-alive`](https://en.wikipedia.org/wiki/HTTP_persistent_connection).
Aussi, afin de traiter un maximum de messages, on ne monopolise pas le thread de traitement de connexion avec la lecture du flux TCP. Pour cela, on utilise NIO.

J'ai ainsi benchmarké différents frameworks :
  - [Vert.x](https://github.com/glandais/hackaton-iot/blob/2d180877a35b7beb7fb9e282c278bf52a70c7dd7/hackaton-glandais-server/src/main/java/com/capgemini/csd/hackaton/server/ServerVertx.java)
  - [RestExpress](https://github.com/glandais/hackaton-iot/blob/2d180877a35b7beb7fb9e282c278bf52a70c7dd7/hackaton-glandais-server-restexpress/src/main/java/com/capgemini/csd/hackaton/server/ServerRestExpress.java)
  - [Netty](https://github.com/glandais/hackaton-iot/blob/master/src/main/java/com/capgemini/csd/hackaton/server/ServerNetty.java)
  - [Undertow](https://github.com/glandais/hackaton-iot/blob/master/src/main/java/com/capgemini/csd/hackaton/server/ServerUndertow.java)

Le dernier s'est montré le plus véloce, avec presque 20000 messages par secondes (sans traitement métier).

### Stockage des messages

On ne parlera ici que du timestamp, du sensorType et de la value. Il n'y a pas de collision d'id et celui ci n'est pas utilisé pour les synthèses.

#### Stockage en mémoire

On stocke les message sous forme de map triée par clé :
 - clé : timestamp + discrimant (si deux messages dans la même milliseconde)
 - valeur : sensorType + value
Ces deux éléments étant chacun composé de deux nombres entiers, j'ai dans un premier temps utilisé des UUID (deux long) plutôt que de créer mes propres structures de données.

Avec Java 8, le code pour obtenir la synthèse est assez compact :

```
	Stream<Value> stream = map.subMap(fromTs, toTs).values().stream();
	Map<Integer, Summary> result = stream.collect(groupingBy(Value::getSensorId,
			mapping(Value::getValue, Collector.of(Summary::new, Summary::accept, Summary::combine2))));
```

#### Stockage persistant

Il y a deux typologies dans ce stockage :
 - le stockage "à persister", une sorte de file de messages à indexer/stocker
 - le stockage pouvant être requêté pour les summaries

##### Files de messages

J'ai obtenu d'excellentes performances avec ChronicleQueue. Malheureusement, après un premier essai sur le Raspberry, [cette librairie n'est pas compatible avec l'architecture ARM](https://github.com/OpenHFT/Chronicle-Queue/issues/253)...

Avec MapDB, les résultats étaient assez médiocres... Je l'avais sûrement mal configuré, car j'ai obtenu des bonnes perfs au final.

Sur une [base de code](https://github.com/flaviovdf/spiderpig/blob/master/src/br/ufmg/dcc/vod/spiderpig/common/queue/basequeues/MemoryMappedFIFOQueue.java), j'ai enfin pu créer une file assez efficace mais j'en avais plus vraiment besoin...

##### Stockage persistant

J'ai testé de nombreuses librairies :
  - H2
  - ObjectDB
  - MapDB
  - ElasticSearch
  - Lucene

Au final j'ai utilisé MapDB, avec des fichiers memorymapped. Simple et efficace, son principal avantage étant d'avoir la même interface que la TreeMap utilisée pour stocker les messages en mémoire.

### Première version

Lorsqu'un message était reçu, il était poussé sur la queue et envoyé à l'indexation. La synthèse était directement demandée à l'indexeur.
Lorsque le serveur était redémarré, toute la queue était réindexée.
Ainsi, les messages n'étaient pas stockés en mémoire, on attendait la fin de l'indexation pour chaque message pour répondre 200, avec les attentes des locks.
Bref, c'était lent.

### Deuxième version

Afin d'être le plus réactif possible, on stocke les messages en mémoire dans une certain limite.
Les nouveaux messages étaient indexées en mémoire (Mem) et ajoutés à une file d'attente (Queue).
Qunad nécessaire (nombre de messages en mémoires, durée, ...), les messages sont transférées sur le disque et supprimés de la mémoire.
La synthèse combine alors les données en mémoire et du disque.

### Calcul des synthèses depuis le disque

Globalement, les stockages sur le disque sont assez faibles pour obtenir des synthèses. Il faudrait une base comme fluxdb pour avoir des résultats corrects, ne demandant pas de scanner toutes les lignes de la plage demandée.

### Troisième version

On change ici de paradigme, en travaillant sur des intervalles.
Chaque intervalle est représenté par :
 - une map de messages (en mémoire ou sur le disque)
 - une synthèse
Lorsqu'un message est reçu, il est ajouté à la map et à la synthèse de son intervalle.

Lorsqu'une synthèse est demandée, il couvre un certain nombre d'intervalles, en totalité ou partiellement.
 - partiel : calcul de la synthèse sur la période demandée
 - complet : utilisation de la synthèse calculée
On combine toutes ces synthèses et on obtient le bon résultat.

On conserve un certain nombre d'intervalles en mémoire (LoadingCache Guava). Quand un intervalle est déchargé de la mémoire, on le laisse accessible en lecture depuis la mémoire mais on bloque en écriture tant que le fichier n'est pas terminé.
A la demande d'un intervalle en écriture, on le créé en mémoire s'il n'existe pas sur le disque.

Cette version supporte assez mal les redémarrages, n'ayant pas eu le temps de travailler sur ce sujet... Il faudrait stocker les synthèses en même temps que les messages, afin de garantir l'intégrité des résultats de synthèse par rapport aux messages stockés sur le disque.

C'est cette version qui a été utilisée pour la finale.

### Optimisations

Paramètres JVM (GC, ...)
 - utilisation des paramètres donnés par [netty-queue](https://github.com/mitallast/netty-queue)

Warming (JIT, HTTP)
 - démarrage d'un serveur sur un port autre que 80, envoie de 100000 messages et demande de synthèses
 - le JRE va compiler et inliner les méthodes pour optimiser le temps d'exécution
 - "The Java/NIO engine start up introduces an overhead on the first request to be executed. In order to compensate this effect, Gatling automatically performs a request to http://gatling.io." http://gatling.io/docs/2.2.1/http/http_protocol.html?highlight=warm

Parsing JSON
 - il est inutile de parser le JSON en map ou en bean, on connait le format
 - utilisation d'un parser JSON plus ou moins event based : moshi

Locks
 - Uilisation de ReentrantLocks et de ReentrantReadWriteLocks plutôt que des blocs synchronized
