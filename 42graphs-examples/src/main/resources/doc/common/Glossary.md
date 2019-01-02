Glossary
========

<a name="alphabet">Alphabet</a>
---------------------------------
Set of distinct [values](#value) in a particular [dimension](#dimension). More formally, [the set of symbols composing the original input sequence][3].

<a name="content_words">Content Words</a>
--------------------------------------------
[Content words](https://en.wikipedia.org/wiki/Content_word) "name objects of reality and their qualities". 
As opposed to the [function words](#function_words) which establish (grammatical) relations between the content words.
They belong to the [open classes](https://en.wikipedia.org/wiki/Part_of_speech#Open_and_closed_classes).
There are way more content words in a language than function words.
But compared to function words content words occur less often in texts.


<a name="corpus">Corpus</a>
---------------------------------
A [text corpus](https://en.wikipedia.org/wiki/Text_corpus) consisting of one or more [documents](#document).

<a name="cypher">Cypher</a>
---------------------------------
[Cypher](https://neo4j.com/cypher-graph-query-language/) is [Neo4j's](#neo4j) query language.


<a name="dimension">Dimension</a>
---------------------------------
In the context of the current project, the term [dimension](https://en.wikipedia.org/wiki/Dimension) is is sometimes used in a narrow sense to refer to the dimension of different [value](#value) types. 
The [graph](#graph) model explored in this project is basically a space of coordinates derived from a set of source sequences in which each [occurrence](#occurrence) of something can be uniquely identified along three dimensions (in the wider sense): 
The [value](#value) type, the containing element and the position of the occurrence in the containing element. 
(Neglecting the fact that value types don't have an intrinsically meaningful order. We can always enumerate them.)

<a name="directNeighbourCount">Direct Neighbour Count</a>
----------------------------------------------------------
Number of distinct adjacent left and right neighbours of a [token](#token) in a [corpus](#corpus).


<a name="document">Document</a>
---------------------------------
A complete natural language text composed of one or more [sentences](#sentence) neglecting any markup. See also the [Wikipedia article](https://en.wikipedia.org/wiki/Document).

<a name="edge">Edge</a>
-----------------------
An [edge](https://en.wikipedia.org/wiki/Glossary_of_graph_theory_terms#edge) connects two [vertexes](#vertex) in a [graph](#graph).

<a name="function_words">Function Words</a>
--------------------------------------------
[Function words](https://en.wikipedia.org/wiki/Function_word) grammatically glue the so-called [content words](#content_words) together.
They belong to the [closed classes](https://en.wikipedia.org/wiki/Part_of_speech#Open_and_closed_classes).
There are only few function words in a language.
But they are used way more often in texts than content words.

<a name="graph">Graph</a>
---------------------------------
In theory just a [graph in the mathematical sense][1]. Technically a structure in a [Neo4j](#neo4j) graph database.

<a name="lemmatization">Lemmatization</a>
------------------------------------------
Let's just go with the [definition by Stanford NLP](https://nlp.stanford.edu/IR-book/html/htmledition/stemming-and-lemmatization-1.html).


<a name="neo4j">Neo4j</a>
--------------------------
[Neo4j](https://neo4j.com/) is a Java-based graph database. 

<a name="node">Node</a>
------------------------
A node basically corresponds to a [vertex](#vertex). See also [Neo4j](#neo4j)'s [graph database terminology](#https://neo4j.com/developer/graph-database/).
In the [graph](#graph) model explored in the current project all nodes correspond to [values](#value).

<a name="occurrence">Occurrence</a>
------------------------------------
A concrete appearance of a [value](#value) at a particular position in the source [sequence](https://en.wikipedia.org/wiki/Sequence).
E.g. a particular [token](#token) at position *n* in a [sentence](#sentence). Where a sentence is itself uniquely identified by its position *m* in a [document](#document).



<a name="relationship">Relationship</a>
------------------------
Connects two [nodes](#node). Corresponds to an [edge](#edge) in [Neo4j](#neo4j)'s [graph database terminology](#https://neo4j.com/developer/graph-database/).

<a name="sentence">Sentence</a>
-------------------------------- 
A "unit of written texts delimited by graphological features such as upper case letters and markers such as periods, question marks, and exclamation marks" as [explained on Wikipedia](https://en.wikipedia.org/wiki/Sentence_%28linguistics%29). A sequence of one or more [tokens](#token).

<a name="token">Token</a>
--------------------------
The smallest meaningful units of language, including punctuation since we are dealing with written language. 
Basically following the [definition on Wikipedia](https://en.wikipedia.org/wiki/Lexical_analysis#Token).

<a name="value">Value</a>
--------------------------
Something unique. Formally a [symbol][4] in an [alphabet](#alphabet). 
In our [graph](#graph) model all [nodes](#node) correspond to values.

<a name="vertex">Vertex</a>
----------------------------
The fundamental unit of [graphs](#graph) in the mathematical sense. See also the [definition on Wikipedia][2]. 

<a name="vocabulary">Vocabulary</a>
------------------------------------
More intuitive term for [alphabet](#alphabet) when it comes to words. 



[1]:https://en.wikipedia.org/wiki/Graph_(discrete_mathematics)#Graph
[2]:https://en.wikipedia.org/wiki/Vertex_(graph_theory)
[3]:https://en.wikipedia.org/wiki/Alphabet_(formal_languages)
[4]:https://en.wikipedia.org/wiki/Symbol_(programming)