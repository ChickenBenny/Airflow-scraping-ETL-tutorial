-- create a table to store the data extract from youtube
CREATE TABLE IF NOT EXISTS investment_data (
    dt TEXT NOT NULL,
    dealer_buy TEXT NOT NULL,
    dealer_sell TEXT NOT NULL,
    dealer_dif TEXT NOT NULL,
    investment_buy TEXT NOT NULL,
    investment_sell TEXT NOT NULL,
    investment_dif TEXT NOT NULL,
    foreign_buy TEXT NOT NULL,
    foreign_sell TEXT NOT NULL,
    foreign_dif TEXT NOT NULL
); 