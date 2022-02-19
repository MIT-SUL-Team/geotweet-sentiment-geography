from num_posts_summary_script import *
from missing_file_report_script import *
from sentiment_graph_script import *
from fpdf import FPDF

if __name__ == '__main__':

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(40, 10, 'Hello World!')
    pdf.image("/home/sirenay/3_out/daily_num_posts_graph_2012.png", x=None, y=None, w=0, h=0, type='', link='')
    pdf.output('/home/sirenay/3_out/tuto1.pdf', 'F')


    for year in range(2012, 2013):
        geo_dir = "/srv/data/twitter_geography/"
        sent_dir = "/srv/data/twitter_sentiment/"
        pipeline_dir = "/home/sirenay/2_pipeline"
        generate_daily_num_posts_df_year(year, geo_dir, sent_dir, pipeline_dir)

        out_dir = "/home/sirenay/3_out/"
        generate_daily_num_posts_graph_year(year, pipeline_dir, out_dir)

        # for folder in ["geography", "sentiment"]:
            # saving_list_of_files(year, folder, geo_dir, sent_dir, pipeline_dir)



