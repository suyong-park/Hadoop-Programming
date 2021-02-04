## Using MultipleOutputs in Analyzing Airline Data
`GenericOptionsParser` has a fatal problem. They fail to process the two data in parallel.

So, we can use `MultipleOutputs`. This can be made a multiple outputs files.

`MultipleOutputs` is can be process data in parallel. 

### Using
* Make `MultipleOutputs` Object in setup method (Reducer Class). 
* Instead of Context, using `MultipleOutputs.write()`
* Parameter is an ("output directory name", outputkey, output value)

### Results
![img.png](img.png)