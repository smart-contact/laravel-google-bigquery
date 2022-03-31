<?php
namespace SmartContact\LaravelGoogleBigQuery\Service;

use Google\Cloud\Core\Exception\NotFoundException;
use SmartContact\LaravelGoogleBigQuery\Exceptions\BigQueryInvalidRowException;
use Google\Cloud\BigQuery\BigQueryClient;
use Illuminate\Support\Facades\Log;

class BigQuery
{
    protected $projectId;
    protected $datasetId;
    protected $dataset;
    protected $bigQuery;

    public function __construct($projectId, $datasetId)
    {
        $this->projectId = $projectId;
        $this->datasetId = $datasetId;
        $this->bigQuery = new BigQueryClient([
            'projectId' => $this->projectId,
            'keyFilePath' => config('google_bigquery.credentials')
        ]);

        $this->dataset = $this->bigQuery->dataset($this->datasetId);
    }

    public function insertRows($tableId, $data)
    {
        $table = $this->dataset->table($tableId);

        //formato $data = [['data' => $row1], ['data' => $row2]...['data' => $rowN]];
        $insertResponse = $table->insertRows($data);

        if (! $insertResponse->isSuccessful()) {
            foreach ($insertResponse->failedRows() as $row) {
                foreach ($row['errors'] as $error) {
                    Log::error($tableId . " - " . $error['reason'] . ":" .$error['message']);
                    throw new BigQueryInvalidRowException();
                }
            }
        }
    }

    public function createTable($tableId, $fields, $refresh = FALSE)
    {
        if($this->dataset->table($tableId)->exists() && ! $refresh) {
            return;
        }

        if($refresh) {
            try {
                $this->dataset->table($tableId)->delete();
            } catch (NotFoundException $e) {
                echo "Table $tableId Not Found\n";
            }

        }

        $schema = ['fields' => $fields];
        $this->dataset->createTable($tableId, ['schema' => $schema]);
    }

    public function insert($tableId, $data)
    {
        $fields = implode(',', array_keys($data['data']));
        $values = '';
        foreach ($data['data'] as $key => $value) {
            switch (gettype($value)) {
                case 'boolean':
                case 'double':
                case 'integer':{
                    if($key === 'inserted_to_datawarehouse') {
                        $value = intval($value);
                    }
                    $values .= "{$value},"; break;
                }
                case 'NULL': {
                    $values .= "NULL,"; break;
                }
                default: $values .= "'" . addslashes($value) . "',";
            }
        }
        $values = substr($values, 0, -1);

        $sql = "INSERT INTO {$this->datasetId}.{$tableId} ({$fields}) VALUES($values)";
        try {
            $queryJobConfig = $this->bigQuery->query($sql);
            $queryResults = $this->bigQuery->runQuery($queryJobConfig);
            return $queryResults->isComplete();
        } catch (\Exception $e) {
            Log::emergency('------ BigQuery ------');

            Log::emergency($e->getMessage());
        }

    }

    public function update($tableId, $condition, $record)
    {
        $values = '';

        foreach ($record['data'] as $key => $value) {

            switch (gettype($value)) {
                case 'boolean':
                case 'double':
                case 'integer':
                {
                    $values .= "$key = {$value},";
                    break;
                }
                case 'NULL': {
                    $values .= "$key = NULL,"; break;
                }
                case 'string': {
                    if($value == '') {
                        $values .= "$key = NULL,"; break;
                    }
                }
                default:
                    $values .= "$key = '" . addslashes($value) . "',";
            }
        }

        $values = substr($values, 0, -1);

        $sql = "UPDATE {$this->datasetId}.{$tableId} SET $values WHERE {$condition}";

        $queryJobConfig = $this->bigQuery->query($sql);
        $queryResults = $this->bigQuery->runQuery($queryJobConfig);

        return $queryResults->isComplete();
    }

    public function updateOrCreate($tableId, $condition, $data)
    {
        $where = '';
        foreach ($condition as $key => $value) {
            switch (gettype($value)) {
                case 'boolean':
                case 'integer':
                case 'double':{

                    if($key === 'inserted_to_datawarehouse') {
                        $value = intval($value);
                    }
                    $where .= "$key = {$value},"; break;
                }

                default: $where .= "$key = '" . addslashes($value) . "' AND ";
            }
        }
        $where = substr($where, 0, -5);
        $sql = "SELECT COUNT(*) as total FROM {$this->projectId}.{$this->datasetId}.{$tableId} WHERE {$where}";

        $existJobConfig = $this->bigQuery->query($sql);

        try {
            foreach ($this->bigQuery->runQuery($existJobConfig)->rows() as $index => $row) {
                $exist = $row['total'];
            }
        } catch (\Exception $e) {
            throw new \Exception();
        }


        if($exist) {
            return $this->update($tableId, $where, ['data' => $data]);
        } else {
            return $this->insert($tableId, ['data' => array_merge($condition,$data)]);
        }

    }

}
