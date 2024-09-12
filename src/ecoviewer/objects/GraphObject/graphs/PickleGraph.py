from ecoviewer.objects.GraphObject.GraphObject import GraphObject
from ecoviewer.objects.DataManager import DataManager

class PickleGraph(GraphObject):
    def __init__(self, dm : DataManager, title : str = "Pre-saved Graph", pkl_file_name :str = None):
        self.pkl_file_name = pkl_file_name
        super().__init__(dm, title)

    def create_pkl_file_name(self, dm : DataManager):
        return self.pkl_file_name
    
    def create_graph(self, dm : DataManager):
        return self.get_error_msg(f"Could not generate load pre-saved graph {self.pkl_file_name}")