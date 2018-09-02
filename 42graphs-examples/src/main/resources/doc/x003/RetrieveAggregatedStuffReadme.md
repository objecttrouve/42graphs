Shortcuts...
============

Queries get more complex. It makes sense to preaggregate some shortcuts. 
Assume we [warmed up](../x000/WarmUpReadme.md) and slurped a text into the database. 
Let's aggregate the count of distinct neighbours for each [token](../common/Glossary.md#token)...

Aggregate the `distinctNeighbourCount`
--------------------------------------

### Cypher

The naive [Cypher](../common/Glossary.md#cypher) query to do the aggregation looks like this:

    MATCH (o:Token)-[spos]->(:Sentence)<-[vpos]-(n:Token) 
    WHERE vpos.position=spos.position+1 OR vpos.position=spos.position-1
    WITH o AS occurrence, count(distinct n) AS neighbourCount 
    SET occurrence.directNeighbourCount = neighbourCount 

Let's worry about performance later. 

Programmatically
----------------

### Run

Here's how it's done: 

```
!INCLUDE "../../../java/org/objecttrouve/fourtytwo/graphs/examples/x003/retrieve/aggregated/RetrieveAggregatedStuffMain.java"
```

Run `org.objecttrouve.fourtytwo.graphs.examples.x003.retrieve.aggregated.RetrieveAggregatedStuffMain.main` to aggregate neighbour counts and see a sorted list. 

Anything Interesting?
---------------------

Apparently, the words with the largest number of neighbours are [function words](../common/Glossary.md#function_words).
Which is not so surprising given that function words are the most frequent by themselves. 
But it corroborates the intuition that glue together many distinct words, whereas [content words](../common/Glossary.md#content_words), which are the ones being glued, have fewer neighbours. Namely the glue.