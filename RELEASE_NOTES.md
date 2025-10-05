Release Notes

## OAF version 1.0.5
        * [SER-3757] - Switch CI to GitHub Actions
## OAF version 1.0.4
        * [SER-2890] - Refactor pay endpoint to check if provided transaction id is unique in the system
        * [SER-2890] - Refactor transaction status endpoint to return OAF reference as receipt number in the  response
## OAF version 1.0.3
        * [SER-2890] - Add response body to the pay endpoint

## OAF version 1.0.2
        * [SER-2514] - Refactor settlement flow to use TNM reference number as the transaction id
        * [SER-2514] - Fix issue of payer and payee not being saved in Ops App + Fix bug of pay flow not working if oafReference is not passed
## OAF Version 1.0.1
        * [SER-2514] - Fix exceptions when phone number or business code are not provided in pay request + Add error management

## OAF Version 1.0.0
        * [SER-2040] - Project initiation: Add validation, pay and confirm routes
