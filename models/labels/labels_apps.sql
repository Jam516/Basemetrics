{{ config
(
    materialized = 'table'
)
}}

SELECT name, address, category
FROM (VALUES
('Sigmabot', '0x743f2f29cdd66242fb27d292ab2cc92f45674635', 'telegram bot'),
('Sigmabot', '0xecb03b9a0e7f7b5e261d3ef752865af6621a54fe', 'telegram bot'),
('Uniswap', '0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad', 'DEX'),
('Uniswap', '0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24', 'DEX'),
('Uniswap', '0x2626664c2603336e57b271c5c0b26f421741e481', 'DEX'),
('Banana Gun', '0x1fba6b0bbae2b74586fba407fb45bd4788b7b130', 'telegram bot'),
('Aerodrome', '0x6cb442acf35158d5eda88fe602221b67b400be3e', 'DEX')
) AS x (name, address, category)