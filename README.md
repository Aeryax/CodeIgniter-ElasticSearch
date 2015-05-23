elasticsearch-codeigniter-library
=================================

A small library to make search queries and create and add indexes.


## How to index data with the library
You create data in an array to pass it to elasticsearch. You probably want to specify the Key for the document and show the result. Like this:

	$data = array("name"=>"nisse", "age"=>"14", "sex"=>"male");
	var_dump($this->elasticsearch->add("people", $data));
	
This will save the array to the elasticsearch. "people" is the collection, the index where you want to save it.
