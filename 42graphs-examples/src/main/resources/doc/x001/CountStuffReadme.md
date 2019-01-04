Count Something...
==================

Let's start with something simple. Assume we [warmed up](../x000/WarmUpReadme.md) and slurped a text into the database. What are the first few aggregate figures we might want to gain from the database?

Quantities
-----------

### Number of Values

What's the size of the [vocabulary](../common/Glossary.md#vocabulary) in our corpus?

    MATCH (nodes:Token) RETURN count(nodes)

### Number of Occurrences

And how many [tokens](../common/Glossary.md#token) did we have in the original text when we also count duplicates?

    MATCH (childDimension:Token)-[occursIn]->(parentDimension:Sentence) RETURN count(occursIn)
    
### Number of Token Occurrences

How many times does the word "Freude" occur?

    MATCH (childDimension:Token{identifier:'Freude'})-[occursIn]->(parentDimension:Sentence) RETURN count(occursIn)


Programmatically
----------------

### Run

Here's how it's done: 

```
!INCLUDE "../../../java/org/objecttrouve/fourtytwo/graphs/examples/x001/count/CountStuffMain.java"
```

Run `org.objecttrouve.fourtytwo.graphs.examples.x001.count.CountStuffMain.main` to get the first few quantities. 


What Next?
----------

Let's [retrieve some items](../x002/RetrieveStuffReadme.md) from the graph DB. 