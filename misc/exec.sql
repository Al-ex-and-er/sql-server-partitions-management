  exec sspm.CreatePF
    @PFName   = 'pf_int',
    @Start    = '2020-01-01 00:00:00',
    @Stop     = '2020-01-02 06:00:00',
    @Step     = '-30 minutes',
    @DataType = 'datetime',
    @PrintOnly= 1

