# DataStax Coding Challenge

This challenge is a simplified minimal version of minor part of database storage system:

  * Data Types (`IntType`, `LongType`, `TextType`): data types used for serialization and deserialization 
  * Schema, representing a structure of table: names of keys and columns and their types
  * Row, data entity in this storage system
  * Table iterator, means to iterate over the table

Storage invariants:

  * Parts of the Key can _not_ be empty.
  * Keys have to be always written into the table in _sorted_ order.
  * Given multi-part key, comparison is done in a hierarchical manner, from leftmost to rightmost part of the key, 
  for example given tuples (1, 2, 3) and (1, 4, 1), latter one will be greater, since `1 == 1` and `2 < 4`. 

Beware that invariants may be _not_ imposed in the original code.

Your goal is to: 

1. Implement `TextType`, a variable length text type that will support strings of different length.
2. Implement `MergeIterator`, iterator that can take multiple table iterators and merge them while _preserving the key order_.
That is, if iterator I1 yielding rows with multi-part keys: `{(1, 2), (2, 2)}` and I2 yielding `{(1, 3), (2, 1), (2, 3)}`, 
merge iterator should yield `{(1, 2), (1, 3), (2, 1), (2, 2), (2,3)}`). Keep in mind that all supplied iterators _have to be sorted_. 
3. Implement `AsyncMergeIterator` that has a shorter execution time than `MergeIterator` by making some actions asynchronous and/or parallel. Please provide performance measurements.  One of the ways to do it is to decouple reading from deserialization. Please note that writing an asynchronous merge iterator might require writing your own custom, asynchronous version of `TableIterator`. Make sure to embed failsafe mechanisms 
that would prevent unbounded memory usage. 

Please provide code which you consider to be "production quality."

Describe your solutions for (2) and (3) on a conceptual level shortly when submitting the challenge.

Do not add any extra dependencies to the pom file except for testing.

If you see any bugs or problems with the challenge, please do not hesitate to let us know.

There is no hard deadline for completion/submission of this challenge. 
It should take approximately 4 hours to complete it fully, but we understand that finding time is not always easy. 

# License

Copyright DataStax Inc.

Redistribution in source and binary forms are not permitted.
