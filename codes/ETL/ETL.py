# Mọi Feature phải có hàm tên là "compute"
class BaseFeature:
    def compute(self, df):
        raise NotImplementedError("Subclass must implement compute()") # Chưa viết gì


class FeaturePipeline:
    def __init__(self, feature_list):
        self.feature_list = feature_list

    def feature_engine(self, df):
        for feature in self.feature_list:
            df = feature.compute(df)  
            
        return df