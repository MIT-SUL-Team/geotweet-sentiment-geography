from num_posts_summary_by_country_script import *


if __name__ == '__main__':

    geo_dir = "/srv/data/twitter_geography/"
    sent_dir = "/srv/data/twitter_sentiment/"
    out_dir = "../output/num_posts_summary_by_country/"

    for year in range(2012, 2022):
        generate_daily_num_posts_by_country_df_year(year, geo_dir, sent_dir, out_dir)