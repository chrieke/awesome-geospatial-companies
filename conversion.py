import pandas as pd


def reformat(df):
	categories = {"Earth Observation": ":earth_americas:",
	              "UAV / Aerial": ":airplane:",
	              "GIS / Spatial Analysis": ":globe_with_meridians:",
	              "Digital Farming": ":seedling:",
	              "Webmap / Cartography": ":world_map:",
	              "Satellite Operator": ":artificial_satellite:"}
	df = df.replace({"Category": categories})

	df['Company'] = df.apply(lambda x: f"[{x['Company']}]({x['Website']})", axis=1)
	df["Focus"] = df['Category'] + " " + df['Focus']

	gmaps_url = "https://www.google.com/maps/search/"
	df["Address"] = df.apply(
		lambda x: "".join(y + '+' for y in x['Address'].split(" ")), axis=1)
	df['Address (Click)'] = df.apply(
		lambda x: f"[:round_pushpin: {x['City']}]({gmaps_url}{x['Address']})", axis=1)

	df = df.drop(["Website", "Category", "City", "Address"], axis=1)

	return df


pdf = pd.read_csv("geospatial_companies_map_medium - Companies A-Z.csv")
# display(pdf.head(1))

pdf = reformat(df=pdf)

markdown_string = ""
for country in sorted(pdf.Country.unique()):
	df_country = pdf[pdf['Country'] == country]
	df_country = df_country.drop(["Country"], axis=1)

	country_emoji = {"china": "cn",
	                 "france": "fr",
	                 "germany": "de",
	                 "italy": "it",
	                 "south_korea": "kr",
	                 "spain": "es",
	                 "turkey": "tr",
	                 "uae": "united_arab_emirates",
	                 "usa": "us"}
	flag_emoji = country.lower()
	flag_emoji = flag_emoji.replace(" ", "_")
	if flag_emoji in list(country_emoji.keys()):
		flag_emoji = country_emoji[flag_emoji]

	markdown_string = markdown_string + f"## {country} :{flag_emoji}: \n" + f"{df_country.to_markdown(index=False)} \n\n "

with open("Output.md", "w") as text_file:
	text_file.write(markdown_string)