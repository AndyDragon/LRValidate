//  Copyright (c) LE Ferguson, LLC 2013 
namespace LRValidate
{
    using System.Windows;
    using System.Windows.Input;

    public partial class PickRevalidateTime : Window
    {
        public PickRevalidateTime()
        {
            InitializeComponent();
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            System.Data.DataTable dt = new System.Data.DataTable();
            dt.Columns.Add("Date-Time", System.Type.GetType("System.DateTime"));
            dt.Columns.Add("Count", System.Type.GetType("System.Int32"));
            for (int i = 0; i < MainWindow.DateTimeArray.GetUpperBound(0) && MainWindow.DateTimeArray[i].Count > 0; i++) 
            {
                dt.LoadDataRow(new object[] { MainWindow.DateTimeArray[i].Datetime, MainWindow.DateTimeArray[i].Count }, true); 
            }
            DateTimeGrid.DataContext = dt;
        }

        private void DateTimeGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            ((MainWindow)(MainWindow.ThisWindow)).RevalidateCutoffDateTime.Text = ((System.DateTime)((System.Data.DataRowView)(DateTimeGrid.SelectedItem)).Row.ItemArray[0]).ToString("yyyy-MM-dd HH:mm:ss");
            this.Close(); 
        }
    }
}
