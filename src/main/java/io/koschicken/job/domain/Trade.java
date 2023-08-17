package io.koschicken.job.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Trade {
    private String isin;
    private Long quantity;
    private BigDecimal price;
    private String customer;
}
