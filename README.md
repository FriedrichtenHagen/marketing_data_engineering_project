## TODO
dlt: data extraction

- set the location of the pipeline meta files before running the pipeline
  - find the docs for that
- make sure the duckdb files are in a folder
- handle api limits: https://stackoverflow.com/questions/34160992/fb-ads-api-17-user-request-limit-reached

dbt: transformation

cloud deployment
- add settings for cloud deployment


remove for cloud dep?:     ports:
      - "8501:8501"  # Maps port 8501 in the container to port 8501 on your laptop



Traceback (most recent call last):
  File "/usr/local/lib/python3.9/site-packages/dlt/extract/pipe_iterator.py", line 274, in _get_source_item
    pipe_item = next(gen)
StopIteration

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/usr/local/lib/python3.9/site-packages/dlt/extract/pipe_iterator.py", line 274, in _get_source_item
    pipe_item = next(gen)
  File "/app/dlt_pipeline/facebook_ads/helpers.py", line 84, in get_data_chunked
    chunk = list(itertools.islice(it, chunk_size))
  File "/usr/local/lib/python3.9/site-packages/facebook_business/api.py", line 776, in __next__
    if not self._queue and not self.load_next_page():
  File "/usr/local/lib/python3.9/site-packages/facebook_business/api.py", line 828, in load_next_page
    response_obj = self._api.call(
  File "/usr/local/lib/python3.9/site-packages/facebook_business/api.py", line 336, in call
    raise fb_response.error()
facebook_business.exceptions.FacebookRequestError: 

  Message: Call was not successful
  Method:  GET
  Path:    https://graph.facebook.com/v21.0/act_645973716338497/adcreatives
  Params:  {'limit': 35, 'fields': 'id,name,status,thumbnail_url,object_story_spec,effective_object_story_id,call_to_action_type,object_type,template_url,url_tags,instagram_actor_id,product_set_id', 'summary': 'true', 'after': 'MTIwMjE0MDY5MjU0MjEwNTg0'}

  Status:  500
  Response:
    {
      "error": {
        "code": 1,
        "message": "Please reduce the amount of data you're asking for, then retry your request"
      }
    }


The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/usr/local/lib/python3.9/site-packages/dlt/pipeline/pipeline.py", line 443, in extract
    self._extract_source(
  File "/usr/local/lib/python3.9/site-packages/dlt/pipeline/pipeline.py", line 1191, in _extract_source
    load_id = extract.extract(
  File "/usr/local/lib/python3.9/site-packages/dlt/extract/extract.py", line 417, in extract
    self._extract_single_source(
  File "/usr/local/lib/python3.9/site-packages/dlt/extract/extract.py", line 340, in _extract_single_source
    for pipe_item in pipes:
  File "/usr/local/lib/python3.9/site-packages/dlt/extract/pipe_iterator.py", line 158, in __next__
    pipe_item = self._get_source_item()
  File "/usr/local/lib/python3.9/site-packages/dlt/extract/pipe_iterator.py", line 301, in _get_source_item
    return self._get_source_item()
  File "/usr/local/lib/python3.9/site-packages/dlt/extract/pipe_iterator.py", line 305, in _get_source_item
    raise ResourceExtractionError(pipe.name, gen, str(ex), "generator") from ex
dlt.extract.exceptions.ResourceExtractionError: In processing pipe ad_creatives: extraction of resource ad_creatives in generator get_data_chunked caused an exception: 

  Message: Call was not successful
  Method:  GET
  Path:    https://graph.facebook.com/v21.0/act_645973716338497/adcreatives
  Params:  {'limit': 35, 'fields': 'id,name,status,thumbnail_url,object_story_spec,effective_object_story_id,call_to_action_type,object_type,template_url,url_tags,instagram_actor_id,product_set_id', 'summary': 'true', 'after': 'MTIwMjE0MDY5MjU0MjEwNTg0'}

  Status:  500
  Response:
    {
      "error": {
        "code": 1,
        "message": "Please reduce the amount of data you're asking for, then retry your request"
      }
    }


The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/app/dlt_pipeline/facebook_ads_pipeline.py", line 131, in <module>
    load_all_ads_objects()
  File "/app/dlt_pipeline/facebook_ads_pipeline.py", line 30, in load_all_ads_objects
    info = pipeline.run(fb_ads_source)
  File "/usr/local/lib/python3.9/site-packages/dlt/pipeline/pipeline.py", line 227, in _wrap
    step_info = f(self, *args, **kwargs)
  File "/usr/local/lib/python3.9/site-packages/dlt/pipeline/pipeline.py", line 276, in _wrap
    return f(self, *args, **kwargs)
  File "/usr/local/lib/python3.9/site-packages/dlt/pipeline/pipeline.py", line 707, in run
    self.extract(
  File "/usr/local/lib/python3.9/site-packages/dlt/pipeline/pipeline.py", line 227, in _wrap
    step_info = f(self, *args, **kwargs)
  File "/usr/local/lib/python3.9/site-packages/dlt/pipeline/pipeline.py", line 181, in _wrap
    rv = f(self, *args, **kwargs)
  File "/usr/local/lib/python3.9/site-packages/dlt/pipeline/pipeline.py", line 167, in _wrap
    return f(self, *args, **kwargs)
  File "/usr/local/lib/python3.9/site-packages/dlt/pipeline/pipeline.py", line 276, in _wrap
    return f(self, *args, **kwargs)
  File "/usr/local/lib/python3.9/site-packages/dlt/pipeline/pipeline.py", line 464, in extract
    raise PipelineStepFailed(
dlt.pipeline.exceptions.PipelineStepFailed: Pipeline execution failed at stage extract when processing package 1731328647.4314744 with exception:

<class 'dlt.extract.exceptions.ResourceExtractionError'>
In processing pipe ad_creatives: extraction of resource ad_creatives in generator get_data_chunked caused an exception: 

  Message: Call was not successful
  Method:  GET
  Path:    https://graph.facebook.com/v21.0/act_645973716338497/adcreatives
  Params:  {'limit': 35, 'fields': 'id,name,status,thumbnail_url,object_story_spec,effective_object_story_id,call_to_action_type,object_type,template_url,url_tags,instagram_actor_id,product_set_id', 'summary': 'true', 'after': 'MTIwMjE0MDY5MjU0MjEwNTg0'}

  Status:  500
  Response:
    {
      "error": {
        "code": 1,
        "message": "Please reduce the amount of data you're asking for, then retry your request"
      }
    }






    2024-11-11 19:45:00,658|[WARNING]|14|281472920879136|dlt|helpers.py|retry_on_limit:207|facebook_ads source will retry due to (#80004) There have been too many calls to this ad-account. Wait a bit and try again. For more info, please refer to https://developers.facebook.com/docs/graph-api/overview/rate-limiting#ads-management. with error code 80004