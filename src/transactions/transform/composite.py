import apache_beam as beam

from transactions.transform.transformation import filter_amount, filter_date

class CompositeTransform(beam.PTransform):
  def expand(self, pcoll):
    composite_transform_result = ( 
        pcoll
            | "Filter transaction amount > 20" >> beam.Filter(filter_amount)
            | "Exclude transactions before 2010" >> beam.Filter(filter_date)
            | "Group by date" >> beam.Map(lambda transaction_dict:(transaction_dict["timestamp"].date().isoformat(), transaction_dict["transaction_amount"]))
            | "Sum total by date" >> beam.CombinePerKey(sum)
    )
    return composite_transform_result