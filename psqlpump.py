# coding: utf8
# author : gerard

import sqlalchemy as sal
import geopandas as gpd
import pandas as pd
import json
import os
import glob

class PsqlPump():
    def __init__(self):
        self.config = open("config.json")
        self.param = json.load(self.config)
        self.con_string = "postgresql://{0}:{1}@{2}:{3}/{4}".format(
            self.param['cnx_psql']['db_username'],
            self.param['cnx_psql']['db_password'],
            self.param['cnx_psql']['host'],
            self.param['cnx_psql']['port'],
            self.param['cnx_psql']['db_name']
        )

    def craft_engine(self):
        self.engine = sal.create_engine(self.con_string)

    def pumping_shapefile(self):
        """
        :return:
        """
        for lyr in glob.glob(str(self.param['cnx_globales']['shpdir'] + '/*.shp')):
            self.gdf = gpd.read_file(lyr)
            self.name = os.path.basename(lyr)

            if self.gdf.geometry.geom_type.any() == 'Polygon':
                self.geom_type = 'POLYGON'
                self.gdf_singlepoly = self.gdf[self.gdf.geometry.type == 'Polygon']
                self.gdf_multipoly = self.gdf[self.gdf.geometry.type == 'MultiPolygon']

            elif self.gdf.geometry.geom_type.any() == 'LineString':
                self.geom_type = 'LINESTRING'
                self.gdf_singlepoly = self.gdf[self.gdf.geometry.type == 'LineString']
                self.gdf_multipoly = self.gdf[self.gdf.geometry.type == 'MultiLineString']

            elif self.gdf.geometry.geom_type.any() == 'Point':
                self.geom_type = 'POINT'
                self.gdf_singlepoly = self.gdf[self.gdf.geometry.type == 'Point']
                self.gdf_multipoly = self.gdf[self.gdf.geometry.type == 'MultiPoint']


            for i, row in self.gdf_multipoly.iterrows():
                self.series_geometries = pd.Series(row.geometry)
                df = pd.concat([gpd.GeoDataFrame(row, crs=self.gdf_multipoly.crs).T] * len(self.series_geometries),
                               ignore_index=True)
                df['geometry'] = self.series_geometries
                self.gdf_singlepoly = pd.concat([self.gdf_singlepoly, df])
                self.gdf_singlepoly.reset_index(inplace=True, drop=True)

            self.gdf_singlepoly['geometry'] = self.gdf_singlepoly['geometry'].apply(lambda x: x.wkt)
            print 'UPLOADING - ' + self.name[0:-4] + ' - Switching geometry to WKT - '
            with self.engine.connect() as self.conn, self.conn.begin():
                self.gdf_singlepoly.to_sql(self.name[0:-4], con=self.conn, schema=self.param['cnx_psql']['schema'], if_exists='append', index=False)
                self.update = """UPDATE """ + self.param['cnx_psql']['schema'] + "." + "\"" + self.name[0:-4] + "\"" + " SET geometry = (ST_Force2D(geometry))"""
                self.conn.execute(self.update)
                print "---- SQL ---- " + self.update
                self.alter_table_polygon = """ALTER TABLE """ + self.param['cnx_psql']['schema'] + '."' + self.name[
                                                                                                          0:-4] + """" ALTER COLUMN geometry TYPE Geometry (""" + self.geom_type + """, 2154) USING ST_SetSRID(geometry::Geometry, 2154)"""
                self.conn.execute(self.alter_table_polygon)
                print "---- SQL ---- " + self.alter_table_polygon
                print "--------------------------------------------------------------------------------------------------------------------------------------------------------------"

    def run(self):
        self.craft_engine()
        self.pumping_shapefile()

if __name__ == '__main__':
    PsqlPump()
    PsqlPump = PsqlPump()
    PsqlPump.run()

