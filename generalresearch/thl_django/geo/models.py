from django.db import models


class GeonamePostalCode(models.Model):
    pk = models.CompositePrimaryKey("country_iso", "postal_code")

    country_iso = models.CharField(max_length=2, null=False)
    postal_code = models.CharField(max_length=20, null=False)
    place_name = models.CharField(max_length=180, null=True)

    admin_name_1 = models.CharField(max_length=100, null=True)
    admin_code_1 = models.CharField(max_length=20, null=True)
    admin_name_2 = models.CharField(max_length=100, null=True)
    admin_code_2 = models.CharField(max_length=20, null=True)
    admin_name_3 = models.CharField(max_length=100, null=True)
    admin_code_3 = models.CharField(max_length=20, null=True)

    latitude = models.FloatField(null=False)   # double precision
    longitude = models.FloatField(null=False)
    accuracy = models.SmallIntegerField(null=True)

    # If we want: coords geography(Point, 4326) PostGIS generated column,
    #    we'd need to install postgis on any server that runs this db
    #    (prod and jenkins/testing), (e.g. apt install postgresql-16-postgis-3)

    # from django.contrib.postgres.operations import CreateExtension
    #
    # operations = [
    #     CreateExtension("postgis"),  # put this first, before CreateModel
    #     migrations.CreateModel(...),  # generated
    #     migrations.RunSQL(
    #         sql="""
    #         ALTER TABLE geoname_postalcode
    #            ADD COLUMN coords geography(Point, 4326)
    #                 GENERATED ALWAYS AS (
    #                   ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
    #                     ) STORED;
    #         CREATE INDEX geoname_postalcode_coords_gist
    #             ON geoname_postalcode USING gist (coords);
    #         """,
    #         reverse_sql="ALTER TABLE geoname_postalcode DROP COLUMN
    #     coords;",
    # ),
    # ]

    class Meta:
        db_table = "geoname_postalcode"