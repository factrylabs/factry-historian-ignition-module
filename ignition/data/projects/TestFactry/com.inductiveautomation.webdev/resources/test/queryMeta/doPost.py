def doPost(request, session):
	try:
		body = request['data']
		if hasattr(body, 'decode'):
			body = body.decode('utf-8')
		elif not isinstance(body, str):
			body = str(body)
		data = system.util.jsonDecode(body)

		paths = [str(p) for p in data["paths"]]

		kwargs = {"paths": paths}
		if "startDate" in data and data["startDate"]:
			kwargs["startDate"] = system.date.fromMillis(long(data["startDate"]))
		if "endDate" in data and data["endDate"]:
			kwargs["endDate"] = system.date.fromMillis(long(data["endDate"]))

		result = system.historian.queryMetadata(**kwargs)

		# queryMetadata may return a Results object or a DataSet
		if hasattr(result, 'getColumnCount'):
			# DataSet
			columns = [result.getColumnName(c) for c in range(result.getColumnCount())]
			rows = []
			for r in range(result.getRowCount()):
				row = {}
				for c in range(result.getColumnCount()):
					val = result.getValueAt(r, c)
					if val is None:
						row[columns[c]] = None
					elif hasattr(val, "getTime"):
						row[columns[c]] = val.getTime()
					else:
						row[columns[c]] = str(val)
				rows.append(row)
			return {"json": {
				"success": True,
				"columns": columns,
				"rowCount": len(rows),
				"rows": rows
			}}
		elif hasattr(result, 'getResults'):
			# Results object — iterate result items. queryMetadata returns
			# Results<MetadataPoint>; each item exposes source() (the path) and
			# value() (a PropertySet of the metadata properties: engUnit, datatype,
			# name, ...). Flatten each PropertySet into the row.
			items = list(result.getResults())
			rows = []
			for item in items:
				row = {}
				if hasattr(item, 'source'):
					row["path"] = str(item.source())
				elif hasattr(item, 'getPath'):
					row["path"] = str(item.getPath())
				ps = item.value() if hasattr(item, 'value') else None
				if ps is not None:
					for pv in ps:
						pname = str(pv.getProperty().getName())
						pval = pv.getValue()
						if pval is None:
							row[pname] = None
						elif hasattr(pval, "getTime"):
							row[pname] = pval.getTime()
						else:
							row[pname] = str(pval)
				rows.append(row)
			columns = list(set(k for r in rows for k in r.keys())) if rows else []
			return {"json": {
				"success": True,
				"columns": columns,
				"rowCount": len(rows),
				"rows": rows
			}}
		else:
			return {"json": {
				"success": True,
				"type": str(type(result)),
				"value": str(result),
				"rowCount": 0,
				"columns": [],
				"rows": []
			}}

	except Exception, e:
		import traceback
		return {"json": {"success": False, "error": str(e), "trace": traceback.format_exc()}}
