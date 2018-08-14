from django.shortcuts import render
from django.views.generic.base import View
from search.models import ArticleType
from django.http import HttpResponse, HttpRequest
import json
from datetime import datetime
from elasticsearch import Elasticsearch
import redis
client = Elasticsearch(hosts=["127.0.0.1"])
redis_cli = redis.StrictRedis()


class SearchSuggest(View):
    def get(self, request):
        key_words = request.GET.get('s', '')
        re_datas = []

        if key_words:
            s = ArticleType.search()
            s = s.suggest('my_suggest', key_words, completion={
                "field": "suggest", "fuzzy": {
                    "fuzziness": 2
                },
                "size": 10
            })
            suggestions = s.execute_suggest()
            for match in suggestions.my_suggest[0].options:
                source = match._source
                re_datas.append(source["title"])
        return HttpResponse(json.dumps(re_datas), content_type="application/json")


class SearchView(View):
    def get(self, request):
        key_words = request.GET.get('q', '')
        page = request.GET.get('p', "")

        try:
            page = int(page)
        except:
            page = 1
        jobbole_count = redis_cli.get("jobbole_count")
        start_time: datetime = datetime.now()
        response = client.search(
            index="jobbole",
            body={
                "query": {
                    "multi_match": {
                        "query": key_words,
                        "fields": ["tags", "title", "content"]
                    }
                },
                "from": (page-1) * 10,
                "size": 10,
                "highlight": {
                    "pre_tags": ['<span class="keyWord">'],
                    "post_tags": ['</span>'],
                    "fields": {
                        "title": {},
                        "content": {},
                    }
                }
            }
        )
        end_time: datetime = datetime.now()
        last_seconds: float = (end_time - start_time).total_seconds()
        total_nums = response["hits"]["total"]
        hit_list = []
        for hit in response["hits"]["hits"]:
            hit_dict = {}
            if "title" in hit["highlight"]:
                hit_dict["title"] = "".join(hit["highlight"]["title"])
            else:
                hit_dict["title"] = hit["_source"]["title"]
            if "content" in hit["highlight"]:
                hit_dict["content"] = "".join(hit["highlight"]["content"][:100])
            else:
                hit_dict["content"] = hit["_source"]["content"][:100]
            hit_dict["url"] = hit["_source"]["url"]
            hit_dict["score"] = hit["_score"]
            hit_list.append(hit_dict)

        return render(request, "result.html",
                      {
                        "all_hits": hit_list,
                        "key_words": key_words,
                        "page": page,
                        "total_nums": total_nums,
                        "last_seconds": last_seconds,
                        "jobbole_count": jobbole_count,
                      }
                      )

