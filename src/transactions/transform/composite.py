import apache_beam as beam

class CompositeTransform(beam.PTransform):
    def __init__(self, aggregations):
        self.aggregations = aggregations

    def expand(self, pcoll):
        composite_transform_result = (
            pcoll
            | "Filter transaction amount > 20"
              >> beam.Filter(self.aggregations.filter_amount)
            | "Exclude transactions before 2010"
              >> beam.Filter(self.aggregations.filter_date)
            | "Group by date"
              >> beam.Map(self.aggregations.group_date_amount)
            | "Sum total by date"
              >> beam.CombinePerKey(sum)
        )
        return composite_transform_result
