from typing import Dict


def index_json(additional_fields: Dict = None):
    properties = {
        "f0": {"type": "keyword"},
        "f1": {"type": "keyword"},
        "f2": {"type": "keyword"},
        "f3": {"type": "keyword"},
        "r0": {"type": "long"},
        "r1": {"type": "long"},
        "r2": {"type": "long"},
        "r3": {"type": "long"}
    }
    if additional_fields is not None and len(additional_fields) > 0:
        properties.update(additional_fields)
    return {"properties": properties}


def query_json(d: Dict):
    return {
        "function_score": {
            "boost_mode": "sum",
            "score_mode": "sum",
            "functions": [
                {
                    "script_score": {
                        "script": {
                            "id": d["script_name"],
                            "params": {
                                "field": "r0",
                                "subcode": d["r0"]
                            }
                        }
                    },
                    "weight": 1
                },
                {
                    "script_score": {
                        "script": {
                            "id": d["script_name"],
                            "params": {
                                "field": "r1",
                                "subcode": d["r1"]
                            }
                        }
                    },
                    "weight": 1
                },
                {
                    "script_score": {
                        "script": {
                            "id": d["script_name"],
                            "params": {
                                "field": "r2",
                                "subcode": d["r2"]
                            }
                        }
                    },
                    "weight": 1
                },
                {
                    "script_score": {
                        "script": {
                            "id": d["script_name"],
                            "params": {
                                "field": "r3",
                                "subcode": d["r3"]
                            }
                        }
                    },
                    "weight": 1
                }
            ],
            "query": {
                "constant_score": {
                    "boost": 0,
                    "filter": {
                        "bool": {
                            "minimum_should_match": 1,
                            "should": [
                                {
                                    "terms": {
                                        "f0": {
                                            "id": d["f0"],
                                            "index": d["nbs_index_name"],
                                            "path": "nbs"
                                        }
                                    }
                                },
                                {
                                    "terms": {
                                        "f1": {
                                            "id": d["f1"],
                                            "index": d["nbs_index_name"],
                                            "path": "nbs"
                                        }
                                    }
                                },
                                {
                                    "terms": {
                                        "f2": {
                                            "id": d["f2"],
                                            "index": d["nbs_index_name"],
                                            "path": "nbs"
                                        }
                                    }
                                },
                                {
                                    "terms": {
                                        "f3": {
                                            "id": d["f3"],
                                            "index": d["nbs_index_name"],
                                            "path": "nbs"
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
    }
