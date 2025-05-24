#!/bin/bash

# 等待 Elasticsearch 启动并可用
# The script will be run from a different container, so it targets 'elasticsearch' hostname
until curl -s http://elasticsearch:9200/_cluster/health?wait_for_status=yellow&timeout=10s; do
    echo "Waiting for Elasticsearch to be ready..."
    sleep 5
done

echo "Elasticsearch is up - applying settings, mappings, and scripts"

# 1. 创建 address_places 索引 (Settings and Mappings)
echo "Creating/Updating address_places index..."
curl -X PUT "http://elasticsearch:9200/address_places" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index.max_ngram_diff": 99,
    "index.mapping.total_fields.limit": 100000,
    "analysis": {
      "analyzer": {
        "myanmar_ngram": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": ["lowercase", "my_ngram"]
        }
      },
      "filter": {
        "my_ngram": {
          "type": "ngram",
          "min_gram": 4,
          "max_gram": 9
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "names" : {
        "properties": {
          "name": {
            "type": "text",
            "analyzer": "myanmar_kytea_analyzer",
            "search_analyzer": "myanmar_kytea_analyzer",
            "fields": {
              "ngram": {
                "type": "text",
                "analyzer": "myanmar_ngram",
                "search_analyzer": "myanmar_ngram"
              }
            }
          },
          "name:my": {
            "type": "text",
            "analyzer": "myanmar_kytea_analyzer",
            "search_analyzer": "myanmar_kytea_analyzer",
            "fields": {
              "ngram": {
                "type": "text",
                "analyzer": "myanmar_ngram",
                "search_analyzer": "myanmar_ngram"
              }
            }
          },
          "name:en": {
            "type": "text"
          }
        }
      },
      "address_parts": {
        "type": "nested",
        "properties": {
          "name": {
            "properties": {
              "name:my": {
                "type": "text",
                "analyzer": "myanmar_kytea_analyzer",
                "search_analyzer": "myanmar_kytea_analyzer",
                "fields": {
                  "keyword": {"type": "keyword", "ignore_above": 256},
                  "ngram": {
                    "type": "text",
                    "analyzer": "myanmar_ngram",
                    "search_analyzer": "myanmar_ngram"
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
'
echo ""

# 2. 创建 address_places_search 搜索模板脚本
echo "Creating/Updating _scripts/address_places_search..."
curl -X POST "http://elasticsearch:9200/_scripts/address_places_search" -H 'Content-Type: application/json' -d'
{
  "script": {
    "lang": "mustache",
    "source": "{\"query\":{\"nested\":{\"path\":\"address_parts\",\"query\":{\"function_score\":{\"query\":{\"match\":{\"address_parts.name.name:my\":{\"query\":\"{{keyword}}\"}}},\"functions\":[{\"script_score\":{\"script\":{\"source\":\"Math.pow(2, doc[\\\"address_parts.rank\\\"].value / 5)\"}}}],\"boost_mode\":\"multiply\"}},\"score_mode\":\"avg\",\"inner_hits\":{\"size\":3}}},\"sort\":[\"_score\"],\"size\":\"{{size}}\"}"
  }
}
'
echo ""

# 3. 创建 name_search 搜索模板脚本
echo "Creating/Updating _scripts/name_search..."
curl -X POST "http://elasticsearch:9200/_scripts/name_search" -H 'Content-Type: application/json' -d'
{
  "script": {
    "lang": "mustache",
    "source": "{\"query\":{\"multi_match\":{\"fields\":[\"names.name:my.ngram\",\"names.name.ngram\"],\"query\":\"{{keyword}}\"}},\"size\":\"{{size}}\"}"
  }
}
'
echo ""

# 4. 创建 universal_name_address_search 搜索模板脚本
echo "Creating/Updating _scripts/universal_name_address_search..."
curl -X POST "http://elasticsearch:9200/_scripts/universal_name_address_search" -H 'Content-Type: application/json' -d'
{
  "script": {
    "lang": "mustache",
    "source": "{\"query\":{\"bool\":{\"should\":[{\"multi_match\":{\"fields\":[\"names.name:my.ngram\",\"names.name.ngram\"],\"query\":\"{{keyword}}\"}},{\"nested\":{\"path\":\"address_parts\",\"query\":{\"function_score\":{\"query\":{\"match\":{\"address_parts.name.name:my\":{\"query\":\"{{keyword}}\"}}},\"functions\":[{\"script_score\":{\"script\":{\"source\":\"Math.pow(2, doc[\\\"address_parts.rank\\\"].value / 5)\"}}}],\"boost_mode\":\"multiply\"}},\"score_mode\":\"avg\",\"inner_hits\":{\"size\":3}}}]},\"sort\":[\"_score\"],\"size\":\"{{size}}\"}"
  }
}
'
echo ""
echo "Elasticsearch initialization script finished."
