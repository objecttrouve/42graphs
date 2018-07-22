Count Something...
==================

Let's stick to something simple. Assume we [warmed up](../x000/WarmUpReadme.md) and slurped a text into the database. What would be the first few elements to retrieve from the DB?

Items
-----

### All Distinct Tokens

What [tokens](../common/Glossary.md#token) do we have in our corpus?

    MATCH (nodes:Token) RETURN nodes


Programmatically
----------------

### Run

Here's how it's done: 

```
!INCLUDE "../../../java/org/objecttrouve/fourtytwo/graphs/examples/x002/count/RetrieveStuffMain.java"
```

Run `org.objecttrouve.fourtytwo.graphs.examples.x002.count.RetrieveStuffMain.main` to get the first few items. 

