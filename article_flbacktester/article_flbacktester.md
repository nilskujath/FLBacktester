# A Pandas-based Framework for Rapid Prototyping of Trading Strategies

## Introduction

Backtesting is fundamental to the development and evaluation of trading strategies.
Numerous commercial platforms, along with free and open-source software libraries, exist to facilitate this process.
Nevertheless, many practitioners—particularly those with discretionary trading backgrounds—continue to rely on spreadsheet-based tools or visual chart analysis to validate their ideas.
Despite their intuitive appeal, these methods typically become inadequate when confronted with complex position management scenarios or strategies involving longer historical lookback periods.

When these practitioners recognize the need for more sophisticated backtesting capabilities, they are confronted with the numerous available commercial backtesting platforms and open-source libraries, which each come with their own set of challenges.
Commercial platforms often emphasize user-friendliness, appealing to a less tech-savy audience. This benefit frequently comes at the cost of transparency and flexibility, often making it impossible to accommodate unconventional or evolving requirements.
In this case, existing open-source libraries seem to be the alternative since they could, in theory, be adapted to individual needs.
In practice, however, such adaptation usually proves elusive: meaningful customization generally demands deep familiarity with the framework’s internal architecture, often rendering the process prohibitively costly in terms of both the required time and expertise.
This realisation often leads users to consider building their own systems, a path that is often just as prohibitive - requiring not only significant programming effort, but also a level of software design expertise that many traders will find daunting to acquire.


---

This article offers a middle path. It introduces a lightweight, Pandas-based backtesting framework that sidesteps the structural complexity of full-scale event-driven systems while preserving the flexibility needed to model realistic execution mechanics, including complex order types and nuanced position management. The goal is to bridge the gap between overly simplistic tools and prohibitively complex infrastructures by providing a transparent, easily understandable foundation that empowers users to build and prototype their own backtesting workflows. In doing so, the framework not only facilitates rapid strategy development but also serves as a stepping stone—preparing users to design more advanced systems as their requirements evolve. It aims to lower the barrier to robust backtesting for technically inclined traders without formal software training, while remaining accessible to those who intuitively think in terms of tabular data and visual chart patterns.

---

* the idea is to have a flexible framework such that we do not need to copy and paste or change the code etc and have stuff in parallel, so we subclass the functionality