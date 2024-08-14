import dataclasses
import json
from typing import ClassVar
import pandas as pd
import requests


@dataclasses.dataclass
class OpenAlexDownloader:
    from_year: int
    to_year: int
    concepts: list = None
    email: str = None
    batch_by: str = "YEAR"
    has_abstract: bool = None

    out_path: str = ""

    _API_: ClassVar = "https://api.openalex.org/works"

    # Get a Table of all works in OpenAlex per year
    def __post_init__(self):
        self.auth = ""
        if self.email is None:
            print("Please define your email for faster API calls")

    def get_count_per_year(self):
        result_dict = {}
        for year in range(self.from_year, self.to_year):
            result_dict[year] = requests.get(self.build_request_str()).json()["meta"]["count"]
        df: pd.DataFrame = pd.DataFrame.from_dict(result_dict, orient="index")
        df.to_csv(f"data/openalex_works_count_{self.from_year}-{self.to_year}"
                  ".csv")
        return df

    def build_request_str(self,
                          year: int = 1950, 
                          per_page: int = 100,
                          request_cursor: str = "*"):
        filters_lst = []

        # YEAR
        if year > 1900:
            filters_lst.append(f"publication_year:{year}")

        # CONCEPTS
        concept_filter = "concepts.id:" + "|".join(
            [f"{concept}" for concept in self.concepts]
        )
        filters_lst.append(concept_filter)

        # abstract
        if self.has_abstract is not None:
            filters_lst.append(f"has_abstract:{self.has_abstract}".lower())

        filters = f"filter={','.join(filters_lst)}"
        # auth
        params = []
        if self.email is not None and len(self.email) > 0:
            params.append(f"mailto={self.email}")

        params.append(f"per_page={per_page}")
        params.append(f"cursor={request_cursor}")
        params_str = '&'.join(params)

        request_str = f"{OpenAlexDownloader._API_}?{filters}&{params_str}"
        #print(request_str)
        return request_str

    def download(self, only_overall_stats: bool= False):
        overall_stats = []
        for year in range(self.from_year, self.to_year+1):
            yearly_works = []
            request = requests.get(self.build_request_str(year=year))
            year_count = request.json()["meta"]["count"]
            overall_stats.append({"year": year,
                                  "count": year_count})  # for Overall stats
            if only_overall_stats:
                continue

            yearly_works.extend(request.json()["results"])

            next_cursor = request.json()["meta"]["next_cursor"]
            while next_cursor is not None:
                url = self.build_request_str(year=year,
                                             request_cursor=str(next_cursor))
                request = requests.get(url)
                next_cursor = request.json()["meta"]["next_cursor"]

                yearly_works.extend(request.json()["results"])
            with open(f"{self.out_path}OA_"+str(year)+".json", "w") as j:
                json.dump(yearly_works, j)

<<<<<<< HEAD
            print(f"{len(yearly_works)}/{year_count} for year {year}")
            
=======
            if year_count != len(yearly_works):
                print(f"Expected {year_count} and got {len(yearly_works)}" )
            print(f"{year} had {year_count} articles.")

>>>>>>> b51e715aee46702058e379a8659859bc4b047e07
        overall_stats_df = pd.DataFrame(overall_stats)
        overall_stats_df.to_csv("article_count_per_year.csv")
        return overall_stats_df
