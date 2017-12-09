Advanced Analytics with Spark
=============================

## Predição de cobertura florestal usando árvores de decisão no Apache Spark

### Analise exploratória do Dataset

Use o programa R [/src/main/resources/explorer-with-R.R](/src/main/resources/explorer-with-R.R)
para fazer analise exploratória do Dataset.

Veja a seguir como fazer **build/run** do código para Arvores de Decisão.

### Build & Run

```bash
cd CPD-02
mvn -Drat.ignoreErrors=true \
    -Dcheckstyle.skip \
    -Dmaven.test.skip=true \
    -Denforcer.skip=true \
    clean package install
cd covtype
cd src/main/resources
unzip covtype.csv.zip
cd ../../..
ln -s covtype-full.csv src/main/resources/covtype.csv
java -jar target/covtype-2.2.0-jar-with-dependencies.jar
```
Você verá uma mensagem indicando que é necessário informar pelo menos um parâmetro.
Iso indica que está tudo bem até aqui.

Para rodar sem log algum podemos redirecionar `stderr`

```bash
time java -jar target/covtype-2.2.0-jar-with-dependencies.jar 4 2> /dev/null
```

Este comando acima executa com **dataset pequeno** (apenas 3000 tuplas)
que demora em torno de 1 muto num Intel i5 com 4GB de RAM.

Para tomar os tempos finais com o Dataset completo use:

```bash
time java -jar target/covtype-2.2.0-jar-with-dependencies.jar 4 full 2> /dev/null
```

Foi criada a shell bash `run-with-full-dataset` que pode ser invocada passando o número de cores a serem usados

Por exemplo, passando 4 como mostrado abaixo, o modelo será executado com o dataset completo para 4 cores.

```bash
./run-with-full-dataset 4
```
ou para executar no cluster com um total de 16 cores (ex.: 4 nós sendo cada um com 4 cores) :

```bash
./run-with-full-dataset 16 cluster
```

Ainda em relação ao exemplo acima: Você pode executar `tail -f report_4.txt` em outro terminal
para acompanhar o processamento. Os erros e mensagens de debug ficam em stderr_4.txt

Caso deseje usar `spark-shell` faça:

```bash
spark-shell --packages "br.cefet-rj.eic:covtype:2.2.0" \
            --master spark://aldebaran.eic.cefet-rj.br:7077
```

Dentro do shell Spark execute o codigo Scala mostrado: 

primeiro importe as classes e o atributo estático `numCores`

```scala
import eic.randomforest.RDFRunner
import eic.RunRDF.numCores
```

Agora atualize o número de cores desejado e invoque o programa para executar a versão com dataset completo para 16 cores.

```scala
numCores = 12
var runner = new RDFRunner(spark)
// ATENÇÃO: copie o arquivo covtype-full.csv para todos os nós do Cluster no diretório /tmp
runner.doIt("/tmp/covtype-full.csv") // Usa dataset "covtype-full.csv" com 16 cores
```

copiando o arquivo covtype-full.csv para todos os nós do Cluster no diretório /tmp

```bash
for a in worker1.acme.com worker2.acme.com worker3.acme.com worker4.acme.com
do
  echo copiando covtype-full.csv para $a:/tmp usando o comando scp
  scp covtype-full.csv spark@$a:/tmp
  ssh spark@$a "ls -lA /tmp/*.csv"
done
```

Para verificar se o worker está no ar execute:

```bash
for a in worker1.acme.com worker2.acme.com worker3.acme.com worker4.acme.com
do
  echo Verificando $a
  ping -c 3 $a
done
```
#### Usando a API REST do Spark

Para monitorar o status da aplicação faça:

```bash
curl http://aldebaran.eic.cefet-rj.br:8080 | grep "appId=app-"
```

Pegue o primeiro ID que aparece, por exemplo, `app-20171209144657-0011`,
e passe para a query da API REST como mostrado abaixo:

```bash
curl http://localhost:4040/api/v1/applications/app-20171209144657-0011/jobs | \
     egrep "jobId|name|submissionTime|completionTime|stageIds|status"
```

**OBS:** O Application ID associado a sessão do `spark-shell` pode ser vista logo
em uma das primeiras mensagens que aparecem na console.

Uma visão mais simplificada pode ser vista assim: 

```bash
curl http://localhost:4040/api/v1/applications/app-20171209172618-0013/jobs | \
     egrep "jobId|name|status"
```

que mostra algo como isto abaixo:

Observe que nesta execução a aplicação falhou no job 42 no método `collectAsMap`
da implementação `RandomForest`. O motivo pôde ser visto na console do `spark-shell` e foi
o seguinte: `java.lang.OutOfMemoryError: Java heap space`

```javascript
  "jobId" : 42,
  "name" : "collectAsMap at RandomForest.scala:563",
  "status" : "FAILED",
  "jobId" : 41,
  "name" : "collectAsMap at RandomForest.scala:563",
  "status" : "SUCCEEDED",

  . . . 

  "jobId" : 9,
  "name" : "collectAsMap at RandomForest.scala:910",
  "status" : "SUCCEEDED",
  "jobId" : 8,
  "name" : "count at DecisionTreeMetadata.scala:116",
  "status" : "SUCCEEDED",
  "jobId" : 7,
  "name" : "take at DecisionTreeMetadata.scala:112",
  "status" : "SUCCEEDED",
  "jobId" : 6,
  "name" : "take at Classifier.scala:111",
  "status" : "SUCCEEDED",
  "jobId" : 5,
  "name" : "show at RDFRunner.scala:147",
  "status" : "SUCCEEDED",
  "jobId" : 4,
  "name" : "show at RDFRunner.scala:57",
  "status" : "SUCCEEDED",
  "jobId" : 3,
  "name" : "count at RDFRunner.scala:51",
  "status" : "SUCCEEDED",
  "jobId" : 2,
  "name" : "csv at RDFRunner.scala:34",
  "status" : "SUCCEEDED",
  "jobId" : 1,
  "name" : "csv at RDFRunner.scala:34",
  "status" : "SUCCEEDED",
  "jobId" : 0,
  "name" : "csv at RDFRunner.scala:34",
  "status" : "SUCCEEDED",
```

Para executar com HDFS não esqueça de verificar as váriáveis de ambiente 

```bash
export SPARK_HOME=/usr/local/spark
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export YARN_CONF_DIR=/usr/local/hadoop/etc/hadoop
```

Podemos alterar o arquivo JAR gerado para trocar o nivel de log do Spark de INFO para WARN

```bash
jar -xvf target/covtype-2.2.0-jar-with-dependencies.jar org/apache/spark/log4j-defaults.properties 
vi org/apache/spark/log4j-defaults.properties 
jar -uvf target/covtype-2.2.0-jar-with-dependencies.jar org/apache/spark/log4j-defaults.properties
```

### Fazendo clean-up no repositório local do Maven

Pode ser necessário fazer um clean-up no repositório local do Maven. Use o comando abaixo:

```bash
rm -rf ~/.m2/repository/br/cefet-rj/eic
```

### Livro texto usado no Trabalho

![Advanced Analytics with Spark](aas.png)


### Data Set

- https://archive.ics.uci.edu/ml/machine-learning-databases/covtype/
