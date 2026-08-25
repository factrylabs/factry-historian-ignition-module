def doPost(request, session):
	try:
		body = request['data']
		if hasattr(body, 'decode'):
			body = body.decode('utf-8')
		elif not isinstance(body, str):
			body = str(body)
		data = system.util.jsonDecode(body)

		provider = str(data.get('provider', 'Factry Historian 0.8'))
		tagProvider = str(data.get('tagProvider', 'default'))
		paths = [str(p) for p in data['paths']]

		# values: input is tag-major [[tag1_v1, tag1_v2], [tag2_v1, tag2_v2]]
		# storeTagHistory expects timestamp-major [[t1_tag1, t1_tag2], [t2_tag1, t2_tag2]]
		raw_values = data['values']
		num_tags = len(raw_values)
		num_timestamps = len(raw_values[0]) if num_tags > 0 else 0
		values = []
		for t in range(num_timestamps):
			row = []
			for tag in range(num_tags):
				v = raw_values[tag][t]
				if isinstance(v, float):
					row.append(float(v))
				elif isinstance(v, (int, long)):
					row.append(int(v))
				elif isinstance(v, bool):
					row.append(bool(v))
				else:
					row.append(str(v))
			values.append(row)

		dates = [system.date.fromMillis(long(ts)) for ts in data['timestamps']]

		raw_quals = data.get('qualities', None)
		if raw_quals is not None:
			qualities = [int(q) for q in raw_quals]
		else:
			qualities = [192] * len(dates)

		system.tag.storeTagHistory(
			historyprovider=provider,
			tagprovider=tagProvider,
			paths=paths,
			values=values,
			qualities=qualities,
			timestamps=dates
		)

		return {"json": {"success": True, "pointCount": len(paths) * len(dates)}}

	except Exception, e:
		import traceback
		return {"json": {"success": False, "error": str(e), "trace": traceback.format_exc()}}
