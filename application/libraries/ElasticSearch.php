<?php defined('BASEPATH') or exit('No direct script access allowed'); 

/**
 * Elasticsearch Library
 *
 * @package ElasticSearch
 * 
 */
class ElasticSearch
{

    private $ci;


    /**
     * constructor setting the config variables for server ip and index.
     */
    public function __construct() {
        $this->ci =& get_instance();
        $this->ci->load->library('curl');
        $this->ci->config->load("elastic_search");
    }

    /**
     * Handling the call for every function with curl
     * 
     * @param type $path
     * @param type $method
     * @param type $data
     * 
     * @return type
     * @throws Exception
     */
    private function call($path, $method = 'GET', $data = NULL) {
        if (!$this->ci->config->item('index')) {
            throw new Exception('index needs a value');
        }

        $url = $this->ci->config->item('es_server') . '/' . $this->ci->config->item('index') . '/' . $path;

        $headers = array('Accept: application/json', 'Content-Type: application/json');

        $this->ci->curl->create($url);
        $this->ci->curl->option('HTTPHEADER', $headers);
        $this->ci->curl->option('RETURNTRANSFER', TRUE);
        $this->ci->curl->option('SSL_VERIFYHOST', FALSE);
        $this->ci->curl->option('SSL_VERIFYPEER', FALSE);

        switch($method) {
            case 'GET' :
                break;
            case 'POST' :
                $this->ci->curl->post($data);
                break;
            case 'PUT' :
                $this->ci->curl->put(json_encode($data));
                break;
            case 'DELETE' :
                $this->ci->curl->delete();
                break;
        }

        $response = $this->ci->curl->execute();

        return json_decode($response, TRUE);

    }

    /**
     * create a index with mapping or not
     * 
     * @param json $map
     */
    public function create($map = FALSE)
    {
        if (!$map) {
            $this->call(NULL, 'PUT');
        } else {
            $this->call(NULL, 'PUT', $map);
        }
    }

    /**
     * get status
     * 
     * @return array
     */
    public function status()
    {
        return $this->call('_status');
    }

   /**
     * count how many indexes it exists
     * 
     * @param string $type
     * 
     * @return array
     */
    public function count($type)
    {
        return $this->call($type . '/_count?' . http_build_query(array(NULL => '{matchAll:{}}')));
    }

    /**
     * set the mapping for the index
     * 
     * @param string $type
     * @param json   $data
     * 
     * @return array
     */
    public function map($type, $data)
    {
        return $this->call($type . '/_mapping', 'PUT', $data);
    }

    /**
     * set the mapping for the index
     * 
     * @param type $type
     * @param type $id
     * @param type $data
     * 
     * @return type
     */
    public function add($type, $id, $data)
    {
        return $this->call($type . '/' . $id, 'PUT', $data);
    }

    /**
     * delete a index
     * 
     * @param type $type 
     * @param type $id 
     * 
     * @return type 
     */
    public function delete($type, $id)
    {
        return $this->call($type . '/' . $id, 'DELETE');
    }

    /**
     * make a simple search query
     * 
     * @param type $type
     * @param type $q
     * 
     * @return type
     */
    public function query($type, $q)
    {
        return $this->call($type . '/_search?' . http_build_query(array('q' => $q)));
    }

    /**
     * make a advanced search query with json data to send
     * 
     * @param type $type
     * @param type $query
     * 
     * @return type
     */
    public function advancedquery($type, $query)
    {
        return $this->call($type . '/_search', 'POST', $query);
    }

    /**
     * make a search query with result sized set
     * 
     * @param string  $type  what kind of type of index you want to search
     * @param string  $query the query as a string
     * @param integer $size  The size of the results
     * 
     * @return array
     */
    public function query_wresultSize($type, $query, $size = 999)
    {
        return $this->call($type . '/_search?' . http_build_query(array('q' => $q, 'size' => $size)));
    }

    /**
     * get one index via the id
     * 
     * @param string  $type The index type
     * @param integer $id   the indentifier for a index
     * 
     * @return type
     */
    public function get($type, $id)
    {
        return $this->call($type . '/' . $id, 'GET');
    }

    /**
     * Query the whole server
     * 
     * @param type $query
     * 
     * @return type
     */
    public function query_all($query)
    {
        return $this->call('_search?' . http_build_query(array('q' => $query)));
    }

    /**
     * get similar indexes for one index specified by id - send data to add filters or more
     * 
     * @param string  $type
     * @param integer $id
     * @param string  $fields
     * @param string  $data 
     * 
     * @return array 
     */
    public function morelikethis($type, $id, $fields = FALSE, $data = FALSE)
    {
        if ($data != FALSE && !$fields) {
            return $this->call($type . '/' . $id . '/_mlt', 'GET', $data);
        } else if ($data != FALSE && $fields != FALSE) {
            return $this->call($type . '/' . $id . '/_mlt?' . $fields, 'POST', $data);
        } else if (!$fields) {
            return $this->call($type . '/' . $id . '/_mlt');
        } else {
            return $this->call($type . '/' . $id . '/_mlt?' . $fields);
        }
    }

    /**
     * make a search query with result sized set
     * 
     * @param type $query
     * @param type $size
     * 
     * @return type
     */
    public function query_all_wresultSize($query, $size = 999)
    {
        return $this->call('_search?' . http_build_query(array('q' => $query, 'size' => $size)));
    }

    /**
     * make a suggest query based on similar looking terms
     * 
     * @param type $query
     * 
     * @return array
     */
    public function suggest($query)
    {
        return $this->call('_suggest', 'POST', $query);
    }

}