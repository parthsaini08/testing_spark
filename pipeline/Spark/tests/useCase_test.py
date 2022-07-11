import unittest

from schemaDictionary import schemaDictionary


class test_spark(unittest.TestCase):
    def test_spark(self, spark, transformed_df, useCase):
        expected_schema = schemaDictionary(useCase)
        expected_df = spark.read.json(
            "./output_data/{}.json".format(useCase),
            multiLine=True,
            schema=expected_schema,
        )

        transformed_df.show()
        expected_df.show()

        def field_list(fields):
            return fields.name, fields.dataType

        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)

        # assert
        self.assertTrue(
            res,
            "\nTest failed: The resultant schema doesn't match the expected schema!!",
        )
        # Compare data in transformed_df and expected_df
        self.assertEqual(
            sorted(expected_df.collect()),
            sorted(transformed_df.collect()),
            "\nTest failed: The resultant database has different data than expected!!",
        )
        print("Test case passed!!")
